//! Anthropic Messages adapter (`DESIGN.md` §5.3, §5.4, §7.3).
//!
//! Maps the canonical IR to/from Anthropic's `/messages` shape, injects the required `max_tokens`
//! default (4096), folds `instructions` and system messages into the top-level `system` field, and
//! applies explicit cache-control breakpoints (`DESIGN.md` §7.3).

use serde_json::{Map, Value, json};

use crate::cache::normalize_usage;
use crate::error::CoreError;
use crate::ir::{
    Breakpoint, CacheControl, CacheStrategy, Content, ContentBlock, ImageSource, Item, ProviderId,
    Request, Response, Role, StopReason, ToolChoice, Ttl,
};
use crate::provider::{Provider, anthropic_stop_reason, f32_to_value, model_string};

/// The Anthropic Messages dialect adapter.
pub struct Anthropic;

impl Anthropic {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Anthropic {
    fn default() -> Self {
        Self
    }
}

impl Provider for Anthropic {
    fn provider_id(&self) -> ProviderId {
        ProviderId::Anthropic
    }

    fn dialect(&self) -> crate::provider::Dialect {
        crate::provider::Dialect::Anthropic
    }

    fn build_payload(&self, req: &Request) -> Value {
        let mut system_text: Vec<String> = Vec::new();
        if let Some(s) = &req.instructions {
            system_text.push(s.clone());
        }

        let mut messages: Vec<Value> = Vec::new();
        // Source input index for each emitted message, for `Breakpoint::Message` resolution.
        let mut src: Vec<usize> = Vec::new();
        for (i, item) in req.input.iter().enumerate() {
            match item {
                Item::Message {
                    role: Role::System,
                    content,
                } => {
                    system_text.push(content_to_string(content));
                }
                Item::Message { role, content } => {
                    let r = if matches!(role, Role::Tool) {
                        "user"
                    } else {
                        role_str(*role)
                    };
                    messages.push(json!({ "role": r, "content": build_content(content) }));
                    src.push(i);
                }
                Item::FunctionCall {
                    id,
                    name,
                    arguments,
                } => {
                    // `arguments` (JSON string) → Anthropic `input` (parsed object).
                    let input = parse_json_object(arguments);
                    messages.push(json!({
                        "role": "assistant",
                        "content": [{ "type": "tool_use", "id": id, "name": name, "input": input }]
                    }));
                    src.push(i);
                }
                Item::FunctionCallOutput { call_id, output } => {
                    messages.push(json!({
                        "role": "user",
                        "content": [{ "type": "tool_result", "tool_use_id": call_id, "content": output }]
                    }));
                    src.push(i);
                }
                Item::Reasoning { .. } => {
                    // Dropped unless thinking is enabled (out of v1).
                }
            }
        }

        // Apply explicit cache breakpoints to messages (instructions handled on `system` below).
        if let CacheStrategy::Explicit { breakpoints, ttl } = &req.cache {
            for bp in breakpoints {
                match bp {
                    Breakpoint::Instructions => {}
                    Breakpoint::Message { index } => {
                        if let Some(mi) = src.iter().position(|s| *s == *index as usize) {
                            if let Some(msg) = messages.get_mut(mi) {
                                attach_cache_last_block(msg, *ttl);
                            }
                        }
                    }
                    Breakpoint::Last => {
                        if let Some(last) = messages.last_mut() {
                            attach_cache_last_block(last, *ttl);
                        }
                    }
                }
            }
        }

        let mut body = Map::new();
        body.insert("model".into(), json!(model_string(&req.model)));
        body.insert("messages".into(), Value::Array(messages));
        // max_tokens is required for Anthropic; inject a default if absent (DESIGN.md §5.3).
        body.insert("max_tokens".into(), json!(req.max_tokens.unwrap_or(4096)));

        if !system_text.is_empty() {
            let joined = system_text.join("\n\n");
            let mark_instructions = matches!(&req.cache,
                CacheStrategy::Explicit { breakpoints, .. }
                if breakpoints.iter().any(|b| matches!(b, Breakpoint::Instructions)));
            if mark_instructions {
                let ttl = explicit_ttl(&req.cache);
                body.insert(
                    "system".into(),
                    json!([{ "type": "text", "text": joined, "cache_control": cache_control_json(ttl) }]),
                );
            } else {
                body.insert("system".into(), json!(joined));
            }
        }
        if let Some(t) = req.temperature {
            body.insert("temperature".into(), f32_to_value(t));
        }
        if let Some(p) = req.top_p {
            body.insert("top_p".into(), f32_to_value(p));
        }
        if let Some(stop) = &req.stop {
            if !stop.is_empty() {
                body.insert("stop_sequences".into(), json!(stop));
            }
        }
        if let Some(tools) = &req.tools {
            let arr: Vec<Value> = tools
                .iter()
                .map(|t| {
                    let mut m = Map::new();
                    m.insert("name".into(), json!(t.name));
                    if let Some(d) = &t.description {
                        m.insert("description".into(), json!(d));
                    }
                    m.insert("input_schema".into(), t.input_schema.clone());
                    insert_cache_control(&mut m, &t.cache_control);
                    Value::Object(m)
                })
                .collect();
            body.insert("tools".into(), Value::Array(arr));
        }
        if let Some(tc) = &req.tool_choice {
            body.insert("tool_choice".into(), anthropic_tool_choice(tc));
        }
        if req.stream {
            body.insert("stream".into(), json!(true));
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

        let mut output = Vec::new();
        let mut text_parts: Vec<String> = Vec::new();
        if let Some(content) = body.get("content").and_then(|v| v.as_array()) {
            for block in content {
                match block.get("type").and_then(|v| v.as_str()) {
                    Some("text") => {
                        text_parts.push(
                            block
                                .get("text")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string(),
                        );
                    }
                    Some("tool_use") => {
                        let id = block
                            .get("id")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        let name = block
                            .get("name")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        let input = block
                            .get("input")
                            .cloned()
                            .unwrap_or_else(|| Value::Object(Default::default()));
                        // Anthropic `input` (object) → canonical `arguments` (JSON string).
                        let arguments =
                            serde_json::to_string(&input).unwrap_or_else(|_| "{}".into());
                        output.push(Item::FunctionCall {
                            id,
                            name,
                            arguments,
                        });
                    }
                    _ => {
                        // `thinking` and other block kinds dropped (v1).
                    }
                }
            }
        }
        if !text_parts.is_empty() {
            output.insert(
                0,
                Item::Message {
                    role: Role::Assistant,
                    content: Content::Text(text_parts.join("")),
                },
            );
        }

        let stop_reason = body
            .get("stop_reason")
            .and_then(|v| v.as_str())
            .map(anthropic_stop_reason)
            .unwrap_or(StopReason::Other);
        let usage = body
            .get("usage")
            .map(|u| normalize_usage(ProviderId::Anthropic, u))
            .unwrap_or_default();

        Ok(Response {
            id,
            model,
            provider: ProviderId::Anthropic,
            output,
            stop_reason,
            usage,
        })
    }
}

// --- helpers -----------------------------------------------------------------

fn role_str(role: Role) -> &'static str {
    match role {
        Role::User => "user",
        Role::Assistant => "assistant",
        Role::System | Role::Tool => "user",
    }
}

fn content_to_string(content: &Content) -> String {
    match content {
        Content::Text(s) => s.clone(),
        Content::Blocks(blocks) => blocks
            .iter()
            .filter_map(|b| match b {
                ContentBlock::Text { text, .. } => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join(""),
    }
}

fn build_content(content: &Content) -> Value {
    match content {
        Content::Text(s) => Value::String(s.clone()),
        Content::Blocks(blocks) => Value::Array(blocks.iter().map(build_part).collect()),
    }
}

fn build_part(b: &ContentBlock) -> Value {
    let mut m = Map::new();
    match b {
        ContentBlock::Text {
            text,
            cache_control,
        } => {
            m.insert("type".into(), json!("text"));
            m.insert("text".into(), json!(text));
            insert_cache_control(&mut m, cache_control);
        }
        ContentBlock::Image {
            source,
            cache_control,
        } => {
            m.insert("type".into(), json!("image"));
            m.insert(
                "source".into(),
                match source {
                    ImageSource::Url { url } => json!({ "type": "url", "url": url }),
                    ImageSource::Base64 { media_type, data } => {
                        json!({ "type": "base64", "media_type": media_type, "data": data })
                    }
                },
            );
            insert_cache_control(&mut m, cache_control);
        }
        ContentBlock::ToolUse {
            id,
            name,
            input,
            cache_control,
        } => {
            m.insert("type".into(), json!("tool_use"));
            m.insert("id".into(), json!(id));
            m.insert("name".into(), json!(name));
            m.insert("input".into(), input.clone());
            insert_cache_control(&mut m, cache_control);
        }
        ContentBlock::ToolResult {
            tool_use_id,
            content,
            cache_control,
        } => {
            m.insert("type".into(), json!("tool_result"));
            m.insert("tool_use_id".into(), json!(tool_use_id));
            m.insert("content".into(), build_content(content));
            insert_cache_control(&mut m, cache_control);
        }
    }
    Value::Object(m)
}

fn insert_cache_control(m: &mut Map<String, Value>, cc: &Option<CacheControl>) {
    if let Some(cc) = cc {
        m.insert(
            "cache_control".into(),
            serde_json::to_value(cc).unwrap_or_else(|_| json!({ "type": "ephemeral" })),
        );
    }
}

fn anthropic_tool_choice(tc: &ToolChoice) -> Value {
    match tc {
        ToolChoice::Auto => json!({ "type": "auto" }),
        ToolChoice::None => json!({ "type": "none" }),
        // CC "required" has no direct Anthropic equivalent; "any" forces a tool call.
        ToolChoice::Required => json!({ "type": "any" }),
        ToolChoice::Named { name } => json!({ "type": "tool", "name": name }),
    }
}

/// Parse the `function_call.arguments` JSON string into the Anthropic `input` object.
fn parse_json_object(s: &str) -> Value {
    match serde_json::from_str::<Value>(s) {
        Ok(v @ Value::Object(_)) => v,
        _ => Value::Object(Default::default()),
    }
}

fn explicit_ttl(cache: &CacheStrategy) -> Ttl {
    match cache {
        CacheStrategy::Explicit { ttl, .. } => *ttl,
        _ => Ttl::FiveMinutes,
    }
}

/// Wire `cache_control` for a breakpoint: always carries an explicit `ttl` (DESIGN.md §7.3).
fn cache_control_json(ttl: Ttl) -> Value {
    let t = match ttl {
        Ttl::FiveMinutes => "5m",
        Ttl::OneHour => "1h",
    };
    json!({ "type": "ephemeral", "ttl": t })
}

/// Attach `cache_control` to the last content block of a message, promoting a plain-string
/// content to a single text block first (DESIGN.md §7.3).
fn attach_cache_last_block(message: &mut Value, ttl: Ttl) {
    let cc = cache_control_json(ttl);
    let Some(content) = message.get_mut("content") else {
        return;
    };
    if let Value::String(s) = content {
        let text = std::mem::take(s);
        *content = json!([{ "type": "text", "text": text, "cache_control": cc }]);
    } else if let Value::Array(arr) = content {
        if let Some(obj) = arr.last_mut().and_then(|last| last.as_object_mut()) {
            obj.insert("cache_control".into(), cc);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn req(s: &str) -> Request {
        serde_json::from_str(s).unwrap()
    }

    #[test]
    fn build_basic_and_max_tokens_default() {
        let body = Anthropic.build_payload(&req(
            r#"{"model":"claude-sonnet-4-6","input":[{"type":"message","role":"user","content":"hi"}]}"#,
        ));
        assert_eq!(body["model"], "claude-sonnet-4-6");
        assert_eq!(body["messages"][0]["role"], "user");
        assert_eq!(body["max_tokens"], 4096); // injected default
        assert!(body.get("system").is_none());
    }

    #[test]
    fn build_respects_explicit_max_tokens() {
        let body = Anthropic.build_payload(&req(r#"{"model":"m","max_tokens":128,"input":[]}"#));
        assert_eq!(body["max_tokens"], 128);
    }

    #[test]
    fn build_instructions_become_system() {
        let body = Anthropic.build_payload(&req(
            r#"{"model":"m","instructions":"be brief","input":[{"type":"message","role":"user","content":"hi"}]}"#,
        ));
        assert_eq!(body["system"], "be brief");
    }

    #[test]
    fn build_tool_choice_mappings() {
        for (tc, expected) in [
            (r#""auto""#, "auto"),
            (r#""none""#, "none"),
            (r#""required""#, "any"),
        ] {
            let body = Anthropic.build_payload(&req(&format!(
                r#"{{"model":"m","input":[],"tool_choice":{{"type":{tc}}}}}"#
            )));
            assert_eq!(
                body["tool_choice"]["type"], expected,
                "for tool_choice {tc}"
            );
        }
        let body = Anthropic.build_payload(&req(
            r#"{"model":"m","input":[],"tool_choice":{"type":"named","name":"get_weather"}}"#,
        ));
        assert_eq!(
            body["tool_choice"],
            json!({"type":"tool","name":"get_weather"})
        );
    }

    #[test]
    fn build_function_items() {
        let body = Anthropic.build_payload(&req(r#"{"model":"m","input":[
                {"type":"function_call","id":"c1","name":"f","arguments":"{\"q\":\"sf\"}"},
                {"type":"function_call_output","call_id":"c1","output":"{\"ok\":true}"}
            ]}"#));
        let msgs = &body["messages"];
        // arguments JSON string → input object
        assert_eq!(msgs[0]["role"], "assistant");
        assert_eq!(msgs[0]["content"][0]["type"], "tool_use");
        assert_eq!(msgs[0]["content"][0]["input"], json!({"q":"sf"}));
        assert_eq!(msgs[1]["role"], "user");
        assert_eq!(msgs[1]["content"][0]["type"], "tool_result");
        assert_eq!(msgs[1]["content"][0]["content"], "{\"ok\":true}");
    }

    #[test]
    fn build_images() {
        let body = Anthropic.build_payload(&req(
            r#"{"model":"m","input":[{"type":"message","role":"user","content":[
                {"type":"image","source":{"type":"url","url":"https://x/a.png"}},
                {"type":"image","source":{"type":"base64","media_type":"image/png","data":"QUJD"}}
            ]}]}"#,
        ));
        let parts = &body["messages"][0]["content"];
        assert_eq!(
            parts[0]["source"],
            json!({"type":"url","url":"https://x/a.png"})
        );
        assert_eq!(
            parts[1]["source"],
            json!({"type":"base64","media_type":"image/png","data":"QUJD"})
        );
    }

    #[test]
    fn cache_explicit_instructions_marks_system() {
        let body = Anthropic.build_payload(&req(
            r#"{"model":"m","instructions":"sys","input":[{"type":"message","role":"user","content":"hi"}],
               "cache":{"kind":"explicit","breakpoints":[{"at":"instructions"}]}}"#,
        ));
        assert_eq!(body["system"][0]["type"], "text");
        assert_eq!(
            body["system"][0]["cache_control"],
            json!({"type":"ephemeral","ttl":"5m"})
        );
    }

    #[test]
    fn cache_explicit_last_marks_last_message_block() {
        let body = Anthropic.build_payload(&req(r#"{"model":"m","input":[
                {"type":"message","role":"user","content":"first"},
                {"type":"message","role":"user","content":[{"type":"text","text":"second"}]}
            ],"cache":{"kind":"explicit","breakpoints":[{"at":"last"}]}}"#));
        let msgs = body["messages"].as_array().unwrap();
        // The last message is a blocks array; its last block gets cache_control.
        let last_block = msgs.last().unwrap()["content"]
            .as_array()
            .unwrap()
            .last()
            .unwrap();
        assert_eq!(
            last_block["cache_control"],
            json!({"type":"ephemeral","ttl":"5m"})
        );
        // The first message is untouched.
        assert!(msgs[0]["content"].is_string());
    }

    #[test]
    fn cache_explicit_message_index_targets_right_message() {
        // input[1] is "second"; breakpoint message{index:1} must mark it (promoting string→block).
        let body = Anthropic.build_payload(&req(r#"{"model":"m","input":[
                {"type":"message","role":"user","content":"first"},
                {"type":"message","role":"user","content":"second"}
            ],"cache":{"kind":"explicit","breakpoints":[{"at":"message","index":1}],"ttl":"1h"}}"#));
        let target = &body["messages"][1];
        assert_eq!(target["content"][0]["text"], "second");
        assert_eq!(
            target["content"][0]["cache_control"],
            json!({"type":"ephemeral","ttl":"1h"})
        );
    }

    #[test]
    fn parse_text_response() {
        let resp = Anthropic
            .parse_response(&json!({
                "id":"msg_1","model":"claude-sonnet-4-6",
                "content":[{"type":"text","text":"hello"}],
                "stop_reason":"end_turn",
                "usage":{"input_tokens":10,"output_tokens":3}
            }))
            .unwrap();
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        assert_eq!(resp.output.len(), 1);
        match &resp.output[0] {
            Item::Message { role, content } => {
                assert_eq!(*role, Role::Assistant);
                assert_eq!(content, &Content::Text("hello".into()));
            }
            other => panic!("expected Message, got {other:?}"),
        }
        assert_eq!(resp.usage.input_tokens, 10);
    }

    #[test]
    fn parse_tool_use_and_cache_usage() {
        let resp = Anthropic
            .parse_response(&json!({
                "id":"msg_2","model":"claude-sonnet-4-6",
                "content":[
                    {"type":"text","text":"calling"},
                    {"type":"tool_use","id":"tool_1","name":"get_weather","input":{"q":"sf"}}
                ],
                "stop_reason":"tool_use",
                "usage":{"input_tokens":50,"output_tokens":10,"cache_read_input_tokens":20,"cache_creation_input_tokens":5}
            }))
            .unwrap();
        assert_eq!(resp.stop_reason, StopReason::ToolUse);
        assert_eq!(resp.output.len(), 2);
        // text message first, then the function call.
        assert!(matches!(resp.output[0], Item::Message { .. }));
        match &resp.output[1] {
            Item::FunctionCall {
                id,
                name,
                arguments,
            } => {
                assert_eq!(id, "tool_1");
                assert_eq!(name, "get_weather");
                // input object → JSON string arguments
                let v: Value = serde_json::from_str(arguments).unwrap();
                assert_eq!(v, json!({"q":"sf"}));
            }
            other => panic!("expected FunctionCall, got {other:?}"),
        }
        assert_eq!(resp.usage.cache_read, 20);
        assert_eq!(resp.usage.cache_creation, 5);
        assert_eq!(resp.usage.total_input(), 75); // 50 + 20 + 5
    }

    #[test]
    fn parse_drops_thinking_blocks() {
        let resp = Anthropic
            .parse_response(&json!({
                "id":"msg_3","model":"claude-sonnet-4-6",
                "content":[{"type":"thinking","thinking":"..."},{"type":"text","text":"answer"}],
                "stop_reason":"end_turn",
                "usage":{"input_tokens":1,"output_tokens":1}
            }))
            .unwrap();
        // thinking dropped; only the text message remains.
        assert_eq!(resp.output.len(), 1);
    }
}
