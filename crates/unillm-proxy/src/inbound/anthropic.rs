//! Inbound: Anthropic Messages request body → canonical `Request` (inverse of §5.3).

use std::collections::HashMap;

use serde_json::Value;

use unillm_core::ir::{
    Content, ContentBlock, ImageSource, Item, ModelRef, Request, Role, ToolChoice, ToolDef,
};

pub fn parse_anthropic_request(body: &Value) -> Result<Request, unillm_core::CoreError> {
    let model = body.get("model").and_then(|v| v.as_str()).ok_or_else(|| {
        unillm_core::CoreError::InvalidRequest {
            message: "anthropic request missing 'model'".into(),
        }
    })?;

    let mut input = Vec::new();
    if let Some(messages) = body.get("messages").and_then(|v| v.as_array()) {
        for m in messages {
            let role = m.get("role").and_then(|v| v.as_str()).unwrap_or("user");
            let role_enum = if role == "assistant" {
                Role::Assistant
            } else {
                Role::User
            };
            let content = m.get("content").unwrap_or(&Value::Null);

            let mut blocks: Vec<ContentBlock> = Vec::new();
            match content {
                Value::String(s) => blocks.push(ContentBlock::Text {
                    text: s.clone(),
                    cache_control: None,
                }),
                Value::Array(parts) => {
                    for b in parts {
                        match b.get("type").and_then(|v| v.as_str()).unwrap_or("text") {
                            "text" => blocks.push(ContentBlock::Text {
                                text: b
                                    .get("text")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string(),
                                cache_control: None,
                            }),
                            "image" => blocks.push(ContentBlock::Image {
                                source: parse_image_source(b),
                                cache_control: None,
                            }),
                            "tool_use" => {
                                // Flush any preceding text/image blocks as a message first, so the
                                // canonical order is [Message, FunctionCall] (matches §5.4).
                                flush_message(&mut input, &mut blocks, role_enum);
                                input.push(Item::FunctionCall {
                                    id: s(b, "id"),
                                    name: s(b, "name"),
                                    // `input` object → canonical `arguments` JSON string.
                                    arguments: serde_json::to_string(
                                        b.get("input")
                                            .unwrap_or(&Value::Object(Default::default())),
                                    )
                                    .unwrap_or_else(|_| "{}".into()),
                                });
                            }
                            "tool_result" => {
                                flush_message(&mut input, &mut blocks, role_enum);
                                input.push(Item::FunctionCallOutput {
                                    call_id: s(b, "tool_use_id"),
                                    output: match b.get("content") {
                                        Some(Value::String(s)) => s.clone(),
                                        Some(other) => other.to_string(),
                                        None => String::new(),
                                    },
                                });
                            }
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
            if !blocks.is_empty() {
                input.push(Item::Message {
                    role: role_enum,
                    content: Content::Blocks(blocks),
                });
            }
        }
    }

    Ok(Request {
        model: ModelRef::Alias(model.to_string()),
        instructions: body.get("system").map(parse_system),
        input,
        max_tokens: body
            .get("max_tokens")
            .and_then(|v| v.as_u64())
            .map(|x| x as u32),
        temperature: body
            .get("temperature")
            .and_then(|v| v.as_f64())
            .map(|x| x as f32),
        top_p: body.get("top_p").and_then(|v| v.as_f64()).map(|x| x as f32),
        stop: body
            .get("stop_sequences")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|x| x.as_str().map(String::from))
                    .collect()
            }),
        tools: body
            .get("tools")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(parse_tool).collect()),
        tool_choice: body.get("tool_choice").and_then(parse_tool_choice),
        stream: body
            .get("stream")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        cache: unillm_core::CacheStrategy::Auto,
        metadata: HashMap::new(),
    })
}

/// Flush accumulated text/image blocks as a message item (preserving [Message, tool] order).
fn flush_message(input: &mut Vec<Item>, blocks: &mut Vec<ContentBlock>, role: Role) {
    if !blocks.is_empty() {
        input.push(Item::Message {
            role,
            content: Content::Blocks(std::mem::take(blocks)),
        });
    }
}

fn s(v: &Value, key: &str) -> String {
    v.get(key)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}

fn parse_system(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Array(blocks) => blocks
            .iter()
            .filter_map(|b| b.get("text").and_then(|t| t.as_str()))
            .collect::<Vec<_>>()
            .join(""),
        _ => v.to_string(),
    }
}

fn parse_image_source(b: &Value) -> ImageSource {
    let src = b.get("source");
    match src
        .and_then(|s| s.get("type"))
        .and_then(|v| v.as_str())
        .unwrap_or("url")
    {
        "base64" => ImageSource::Base64 {
            media_type: src
                .and_then(|s| s.get("media_type"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            data: src
                .and_then(|s| s.get("data"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
        },
        _ => ImageSource::Url {
            url: src
                .and_then(|s| s.get("url"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
        },
    }
}

fn parse_tool(t: &Value) -> Option<ToolDef> {
    Some(ToolDef {
        name: s(t, "name"),
        description: t
            .get("description")
            .and_then(|v| v.as_str())
            .map(String::from),
        input_schema: t
            .get("input_schema")
            .cloned()
            .unwrap_or_else(|| Value::Object(Default::default())),
        cache_control: None,
    })
}

fn parse_tool_choice(v: &Value) -> Option<ToolChoice> {
    let t = v.get("type").and_then(|v| v.as_str())?;
    Some(match t {
        "auto" => ToolChoice::Auto,
        "none" => ToolChoice::None,
        "any" => ToolChoice::Required,
        "tool" => ToolChoice::Named { name: s(v, "name") },
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use unillm_core::{Anthropic, Provider};

    #[test]
    fn parse_basic_with_system() {
        let req = parse_anthropic_request(&json!({
            "model": "claude-sonnet-4-6",
            "system": "be brief",
            "messages": [{"role": "user", "content": "hi"}],
            "max_tokens": 100
        }))
        .unwrap();
        assert!(matches!(req.model, ModelRef::Alias(ref s) if s == "claude-sonnet-4-6"));
        assert_eq!(req.instructions.as_deref(), Some("be brief"));
        assert_eq!(req.max_tokens, Some(100));
        assert_eq!(req.input.len(), 1);
    }

    #[test]
    fn parse_tool_use_and_result() {
        let req = parse_anthropic_request(&json!({
            "model": "claude-sonnet-4-6",
            "max_tokens": 100,
            "messages": [
                {"role":"assistant","content":[{"type":"text","text":"calling"},{"type":"tool_use","id":"t1","name":"f","input":{"q":1}}]},
                {"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":"{\"ok\":true}"}]}
            ]
        }))
        .unwrap();
        // assistant text message, then the function call, then the output.
        assert!(matches!(req.input[0], Item::Message { .. }));
        match &req.input[1] {
            Item::FunctionCall {
                id,
                name,
                arguments,
            } => {
                assert_eq!(id, "t1");
                assert_eq!(name, "f");
                assert_eq!(arguments, "{\"q\":1}");
            }
            other => panic!("expected FunctionCall, got {other:?}"),
        }
        match &req.input[2] {
            Item::FunctionCallOutput { call_id, output } => {
                assert_eq!(call_id, "t1");
                assert_eq!(output, "{\"ok\":true}");
            }
            other => panic!("expected FunctionCallOutput, got {other:?}"),
        }
    }

    #[test]
    fn parse_tool_choice_variants() {
        let tc = |ttype: &str| {
            parse_anthropic_request(&json!({
                "model": "m", "max_tokens": 1, "messages": [],
                "tool_choice": {"type": ttype}
            }))
            .unwrap()
            .tool_choice
        };
        assert!(matches!(tc("auto"), Some(ToolChoice::Auto)));
        assert!(matches!(tc("none"), Some(ToolChoice::None)));
        assert!(matches!(tc("any"), Some(ToolChoice::Required)));
        assert!(matches!(tc("tool"), Some(ToolChoice::Named { .. })));
    }

    /// Round-trip: canonical Request → Anthropic payload (core build) → parse back. `instructions`
    /// survive as the `system` field; input items round-trip.
    #[test]
    fn roundtrip_through_anthropic_payload() {
        let original: Request = serde_json::from_str(
            r#"{"model":"claude-sonnet-4-6","instructions":"sys","input":[
                {"type":"message","role":"user","content":"hi"}
            ],"tools":[{"name":"f","input_schema":{"type":"object"}}],"tool_choice":{"type":"auto"}}"#,
        )
        .unwrap();

        let payload = Anthropic.build_payload(&original);
        let recovered = parse_anthropic_request(&payload).unwrap();

        assert_eq!(recovered.instructions.as_deref(), Some("sys"));
        assert_eq!(recovered.input.len(), 1);
        assert_eq!(recovered.tools.as_ref().map(|t| t.len()), Some(1));
        assert!(matches!(recovered.tool_choice, Some(ToolChoice::Auto)));
    }
}
