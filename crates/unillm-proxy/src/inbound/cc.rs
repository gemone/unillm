//! Inbound: OpenAI Chat Completions request body → canonical `Request` (inverse of §5.2).

use std::collections::HashMap;

use serde_json::Value;

use super::{get_str, sampling};
use unillm_core::ir::{
    Content, ContentBlock, ImageSource, Item, ModelRef, Request, Role, ToolChoice, ToolDef,
};

pub fn parse_cc_request(body: &Value) -> Result<Request, unillm_core::CoreError> {
    let model = body.get("model").and_then(|v| v.as_str()).ok_or_else(|| {
        unillm_core::CoreError::InvalidRequest {
            message: "chat completions request missing 'model'".into(),
        }
    })?;

    let mut input = Vec::new();
    if let Some(messages) = body.get("messages").and_then(|v| v.as_array()) {
        for m in messages {
            let role = m.get("role").and_then(|v| v.as_str()).unwrap_or("user");

            if let Some(tool_calls) = m.get("tool_calls").and_then(|v| v.as_array()) {
                if let Some(content) = m.get("content").filter(|v| !v.is_null()) {
                    input.push(Item::Message {
                        role: Role::Assistant,
                        content: parse_content(content),
                    });
                }
                for tc in tool_calls {
                    let function = tc.get("function");
                    input.push(Item::FunctionCall {
                        id: get_str(tc, "id"),
                        name: function
                            .and_then(|f| f.get("name"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        // CC `function.arguments` is already a JSON string — store verbatim.
                        arguments: function
                            .and_then(|f| f.get("arguments"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("{}")
                            .to_string(),
                    });
                }
                continue;
            }

            if role == "tool" {
                input.push(Item::FunctionCallOutput {
                    call_id: get_str(m, "tool_call_id"),
                    output: m
                        .get("content")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                });
                continue;
            }

            let role_enum = match role {
                "system" => Role::System,
                "assistant" => Role::Assistant,
                "tool" => Role::Tool,
                _ => Role::User,
            };
            input.push(Item::Message {
                role: role_enum,
                content: parse_content(m.get("content").unwrap_or(&Value::Null)),
            });
        }
    }

    let (max_tokens, temperature, top_p, stream) = sampling(body);
    Ok(Request {
        model: ModelRef::Alias(model.to_string()),
        instructions: None,
        input,
        max_tokens,
        temperature,
        top_p,
        stop: body.get("stop").map(parse_stop),
        tools: body
            .get("tools")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(parse_tool).collect()),
        tool_choice: body.get("tool_choice").map(parse_tool_choice),
        stream,
        cache: unillm_core::CacheStrategy::Auto,
        metadata: HashMap::new(),
    })
}

fn parse_stop(v: &Value) -> Vec<String> {
    match v {
        Value::String(s) => vec![s.clone()],
        Value::Array(arr) => arr
            .iter()
            .filter_map(|x| x.as_str().map(String::from))
            .collect(),
        _ => Vec::new(),
    }
}

fn parse_content(content: &Value) -> Content {
    match content {
        Value::String(s) => Content::Text(s.clone()),
        Value::Array(parts) => Content::Blocks(parts.iter().filter_map(parse_part).collect()),
        other => Content::Text(other.to_string()),
    }
}

fn parse_part(p: &Value) -> Option<ContentBlock> {
    match p.get("type").and_then(|v| v.as_str()).unwrap_or("text") {
        "image_url" => {
            let url = p
                .get("image_url")
                .and_then(|v| v.get("url"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            Some(ContentBlock::Image {
                source: parse_image_url(url),
                cache_control: None,
            })
        }
        _ => Some(ContentBlock::Text {
            text: p
                .get("text")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            cache_control: None,
        }),
    }
}

/// A CC `image_url` may be a real URL or an inline `data:<media>;base64,<data>` URL (§5.2). The
/// latter decodes to a canonical base64 source so it re-translates correctly to Anthropic.
fn parse_image_url(url: &str) -> ImageSource {
    if let Some(rest) = url.strip_prefix("data:") {
        if let Some((media, data)) = rest.split_once(";base64,") {
            return ImageSource::Base64 {
                media_type: media.to_string(),
                data: data.to_string(),
            };
        }
    }
    ImageSource::Url {
        url: url.to_string(),
    }
}

fn parse_tool(t: &Value) -> Option<ToolDef> {
    let function = t.get("function")?;
    Some(ToolDef {
        name: get_str(function, "name"),
        description: function
            .get("description")
            .and_then(|v| v.as_str())
            .map(String::from),
        input_schema: function
            .get("parameters")
            .cloned()
            .unwrap_or_else(|| Value::Object(Default::default())),
        cache_control: None,
    })
}

fn parse_tool_choice(v: &Value) -> ToolChoice {
    if let Some(s) = v.as_str() {
        return match s {
            "none" => ToolChoice::None,
            "required" => ToolChoice::Required,
            _ => ToolChoice::Auto,
        };
    }
    if let Some(name) = v
        .get("function")
        .and_then(|f| f.get("name"))
        .and_then(|v| v.as_str())
    {
        return ToolChoice::Named {
            name: name.to_string(),
        };
    }
    ToolChoice::Auto
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use unillm_core::{CacheStrategy, ChatCompletions, Provider};

    #[test]
    fn parse_basic() {
        let req = parse_cc_request(&json!({
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": "hi"}],
            "max_tokens": 128,
            "temperature": 0.7
        }))
        .unwrap();
        assert!(matches!(req.model, ModelRef::Alias(ref s) if s == "gpt-4o"));
        assert_eq!(req.max_tokens, Some(128));
        // temperature f32 round-trip; 0.7f32 ~ 0.7
        assert_eq!(req.input.len(), 1);
        match &req.input[0] {
            Item::Message { role, content } => {
                assert_eq!(*role, Role::User);
                assert_eq!(content, &Content::Text("hi".into()));
            }
            other => panic!("expected Message, got {other:?}"),
        }
    }

    #[test]
    fn parse_tools_and_tool_choice() {
        let req = parse_cc_request(&json!({
            "model": "gpt-4o",
            "messages": [],
            "tools": [{"type":"function","function":{"name":"get_weather","parameters":{"type":"object"}}}],
            "tool_choice": {"type":"function","function":{"name":"get_weather"}}
        }))
        .unwrap();
        let tools = req.tools.as_ref().unwrap();
        assert_eq!(tools[0].name, "get_weather");
        assert_eq!(tools[0].input_schema, json!({"type":"object"}));
        match &req.tool_choice {
            Some(ToolChoice::Named { name }) => assert_eq!(name, "get_weather"),
            other => panic!("expected Named, got {other:?}"),
        }
    }

    #[test]
    fn parse_tool_messages() {
        let req = parse_cc_request(&json!({
            "model": "gpt-4o",
            "messages": [
                {"role":"assistant","tool_calls":[{"id":"c1","type":"function","function":{"name":"f","arguments":"{\"q\":1}"}}]},
                {"role":"tool","tool_call_id":"c1","content":"{\"ok\":true}"}
            ]
        }))
        .unwrap();
        assert!(matches!(req.input[0], Item::FunctionCall { .. }));
        assert!(matches!(req.input[1], Item::FunctionCallOutput { .. }));
    }

    /// Round-trip: a canonical Request → CC payload (core build) → parse back here. The recovered
    /// request must match on the fields CC can represent.
    #[test]
    fn roundtrip_through_cc_payload() {
        let original: Request = serde_json::from_str(
            r#"{"model":"gpt-4o","instructions":"be brief","input":[
                {"type":"message","role":"user","content":"hi"},
                {"type":"message","role":"user","content":[{"type":"text","text":"two"}]}
            ],"tools":[{"name":"f","input_schema":{"type":"object"}}],"tool_choice":{"type":"auto"}}"#,
        )
        .unwrap();

        let payload =
            ChatCompletions::new(unillm_core::ProviderId::Openai).build_payload(&original);
        let recovered = parse_cc_request(&payload).unwrap();

        assert_eq!(recovered.model, original.model);
        // `instructions` round-trip as a leading system message → one more input item than original.
        assert_eq!(recovered.input.len(), original.input.len() + 1);
        // instructions round-trip as a leading system message → first input item is a system message.
        match &recovered.input[0] {
            Item::Message { role, content } => {
                assert_eq!(*role, Role::System);
                assert_eq!(content, &Content::Text("be brief".into()));
            }
            other => panic!("expected system Message, got {other:?}"),
        }
        assert_eq!(recovered.tools.as_ref().map(|t| t.len()), Some(1));
        assert!(matches!(recovered.tool_choice, Some(ToolChoice::Auto)));
        // cache strategy is inbound-default (auto), not the original's.
        assert!(matches!(recovered.cache, CacheStrategy::Auto));
    }

    #[test]
    fn parse_image_url_variants() {
        assert!(matches!(
            parse_image_url("https://x/a.png"),
            ImageSource::Url { .. }
        ));
        match parse_image_url("data:image/png;base64,QUJD") {
            ImageSource::Base64 { media_type, data } => {
                assert_eq!(media_type, "image/png");
                assert_eq!(data, "QUJD");
            }
            other => panic!("expected Base64, got {other:?}"),
        }
        // A non-base64 data URL is not decodable → falls back to Url.
        assert!(matches!(
            parse_image_url("data:text/plain,hello"),
            ImageSource::Url { .. }
        ));
    }

    /// A canonical base64 image → CC payload (data URL) → parsed back, recovers Base64 — i.e. the
    /// inbound is the true inverse of §5.2's base64→data-URL rule, so it re-translates to Anthropic.
    #[test]
    fn roundtrip_base64_image_through_cc() {
        let original: Request = serde_json::from_str(
            r#"{"model":"gpt-4o","input":[{"type":"message","role":"user","content":[
                {"type":"image","source":{"type":"base64","media_type":"image/png","data":"QUJD"}}
            ]}]}"#,
        )
        .unwrap();
        let payload =
            ChatCompletions::new(unillm_core::ProviderId::Openai).build_payload(&original);
        let recovered = parse_cc_request(&payload).unwrap();
        match &recovered.input[0] {
            Item::Message {
                content: Content::Blocks(b),
                ..
            } => match &b[0] {
                ContentBlock::Image {
                    source: ImageSource::Base64 { media_type, data },
                    ..
                } => {
                    assert_eq!(media_type, "image/png");
                    assert_eq!(data, "QUJD");
                }
                other => panic!("expected base64 Image, got {other:?}"),
            },
            other => panic!("expected image Message, got {other:?}"),
        }
    }
}
