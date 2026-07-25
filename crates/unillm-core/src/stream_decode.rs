//! Streaming decoders — native SSE frames → canonical [`StreamEvent`] (`DESIGN.md` §6.3, §6.4).
//!
//! Each dialect is a stateful [`StreamDecoder`]: feed it [`SseFrame`]s as they arrive and it emits
//! the canonical events for that frame; call [`StreamDecoder::finish`] at EOF to flush the terminal
//! `completed` event. The HTTP transport (M1.5) drives these incrementally over the upstream byte
//! stream; tests drive them over a fully-parsed frame list.

use serde_json::Value;

use crate::cache::normalize_usage;
use crate::error::CoreError;
use crate::ir::{Content, Item, ProviderId, Response, Role, StopReason};
use crate::provider::{anthropic_stop_reason, cc_finish_to_stop_reason};
use crate::sse::SseFrame;
use crate::stream::{ResponseHeader, StreamEvent};

/// A stateful native→canonical stream decoder.
pub trait StreamDecoder: Send {
    /// Process one SSE frame, returning the canonical events it produced.
    fn feed_frame(&mut self, frame: &SseFrame) -> Vec<StreamEvent>;
    /// Flush the terminal `completed` event (idempotent). Call at EOF / `[DONE]`.
    fn finish(&mut self) -> Vec<StreamEvent>;
}

/// Drive a decoder to completion over a frame list (tests / buffered decode).
pub fn decode_all<D: StreamDecoder>(
    frames: impl IntoIterator<Item = SseFrame>,
    mut decoder: D,
) -> Vec<StreamEvent> {
    let mut out = Vec::new();
    for f in frames {
        out.extend(decoder.feed_frame(&f));
    }
    out.extend(decoder.finish());
    out
}

#[derive(Debug, Default, Clone)]
struct ToolAcc {
    id: String,
    name: String,
    arguments: String,
}

fn s(v: &Value, key: &str) -> String {
    v.get(key)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}

fn n(v: &Value, key: &str) -> u64 {
    v.get(key).and_then(|v| v.as_u64()).unwrap_or(0)
}

// -------------------------------------------------------------------------------------------------
// Chat Completions decoder (DESIGN.md §6.3)
// -------------------------------------------------------------------------------------------------

/// Decodes Chat Completions SSE chunks (`DESIGN.md` §6.3).
pub struct CcDecoder {
    provider: ProviderId,
    header: Option<(String, String)>,
    text: String,
    tools: Vec<ToolAcc>,
    stop_reason: Option<StopReason>,
    usage: Option<Value>,
    done: bool,
}

impl CcDecoder {
    pub fn new(provider: ProviderId) -> Self {
        Self {
            provider,
            header: None,
            text: String::new(),
            tools: Vec::new(),
            stop_reason: None,
            usage: None,
            done: false,
        }
    }
}

impl StreamDecoder for CcDecoder {
    fn feed_frame(&mut self, frame: &SseFrame) -> Vec<StreamEvent> {
        let mut out = Vec::new();
        if frame.is_done_marker() {
            out.extend(self.finish());
            return out;
        }
        let Ok(chunk) = serde_json::from_str::<Value>(&frame.data) else {
            return out;
        };

        if self.header.is_none() {
            let id = s(&chunk, "id");
            let model = s(&chunk, "model");
            self.header = Some((id.clone(), model.clone()));
            out.push(StreamEvent::Created {
                response: ResponseHeader {
                    id,
                    model,
                    provider: self.provider,
                    // CC reports usage only at completion (or [DONE]); none is known at creation.
                    input_usage: None,
                },
            });
            out.push(StreamEvent::OutputItemAdded {
                index: 0,
                item: Item::Message {
                    role: Role::Assistant,
                    content: Content::Text(String::new()),
                },
            });
        }

        if let Some(choice) = chunk.get("choices").and_then(|c| c.get(0)) {
            if let Some(delta) = choice.get("delta") {
                if let Some(content) = delta.get("content").and_then(|v| v.as_str()) {
                    if !content.is_empty() {
                        self.text.push_str(content);
                        out.push(StreamEvent::TextDelta {
                            text: content.to_string(),
                        });
                    }
                }
                if let Some(tool_calls) = delta.get("tool_calls").and_then(|v| v.as_array()) {
                    for tc in tool_calls {
                        let idx = tc.get("index").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
                        if idx >= self.tools.len() {
                            self.tools.resize(idx + 1, ToolAcc::default());
                        }
                        if let Some(id) = tc.get("id").and_then(|v| v.as_str()) {
                            self.tools[idx].id = id.to_string();
                        }
                        if let Some(name) = tc
                            .get("function")
                            .and_then(|f| f.get("name"))
                            .and_then(|v| v.as_str())
                        {
                            self.tools[idx].name = name.to_string();
                        }
                        if let Some(args) = tc
                            .get("function")
                            .and_then(|f| f.get("arguments"))
                            .and_then(|v| v.as_str())
                        {
                            self.tools[idx].arguments.push_str(args);
                            out.push(StreamEvent::ToolCallDelta {
                                id: self.tools[idx].id.clone(),
                                name: self.tools[idx].name.clone(),
                                arguments_delta: args.to_string(),
                            });
                        }
                    }
                }
            }
            if let Some(fr) = choice.get("finish_reason").and_then(|v| v.as_str()) {
                self.stop_reason = Some(cc_finish_to_stop_reason(fr));
            }
        }
        if chunk.get("usage").is_some() {
            self.usage = chunk.get("usage").cloned();
        }
        out
    }

    fn finish(&mut self) -> Vec<StreamEvent> {
        if self.done {
            return Vec::new();
        }
        self.done = true;
        let (id, model) = self.header.clone().unwrap_or_default();
        let mut output = Vec::new();
        let text = std::mem::take(&mut self.text);
        if !text.is_empty() || self.tools.is_empty() {
            output.push(Item::Message {
                role: Role::Assistant,
                content: Content::Text(text),
            });
        }
        for t in std::mem::take(&mut self.tools) {
            if t.id.is_empty() && t.name.is_empty() && t.arguments.is_empty() {
                continue;
            }
            output.push(Item::FunctionCall {
                id: t.id,
                name: t.name,
                arguments: t.arguments,
            });
        }
        let stop_reason = self.stop_reason.unwrap_or(StopReason::EndTurn);
        let usage = self
            .usage
            .take()
            .map(|u| normalize_usage(self.provider, &u))
            .unwrap_or_default();
        vec![StreamEvent::Completed {
            response: Response {
                id,
                model,
                provider: self.provider,
                output,
                stop_reason,
                usage,
            },
        }]
    }
}

/// Decode a full Chat Completions SSE document into canonical events.
pub fn decode_cc(
    provider: ProviderId,
    frames: impl IntoIterator<Item = SseFrame>,
) -> Vec<StreamEvent> {
    decode_all(frames, CcDecoder::new(provider))
}

// -------------------------------------------------------------------------------------------------
// Anthropic decoder (DESIGN.md §6.4)
// -------------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
enum CurBlock {
    Text,
    Tool(usize),
}

/// Decodes Anthropic Messages SSE events (`DESIGN.md` §6.4).
pub struct AnthropicDecoder {
    header: Option<(String, String)>,
    text: String,
    tools: Vec<ToolAcc>,
    text_emitted: bool,
    cur: Option<CurBlock>,
    input_usage: Value,
    out_tokens: u64,
    stop_reason: Option<StopReason>,
    done: bool,
}

impl AnthropicDecoder {
    pub fn new() -> Self {
        Self {
            header: None,
            text: String::new(),
            tools: Vec::new(),
            text_emitted: false,
            cur: None,
            input_usage: Value::Null,
            out_tokens: 0,
            stop_reason: None,
            done: false,
        }
    }

    /// Output index for the i-th tool: 1-based if an assistant text item was emitted, else 0-based.
    fn tool_output_index(&self, i: usize) -> u32 {
        if self.text_emitted {
            i as u32 + 1
        } else {
            i as u32
        }
    }
}

impl Default for AnthropicDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamDecoder for AnthropicDecoder {
    fn feed_frame(&mut self, frame: &SseFrame) -> Vec<StreamEvent> {
        let mut out = Vec::new();
        let event_type = frame.event.as_deref().unwrap_or("");
        let data = if frame.data.is_empty() {
            Value::Null
        } else {
            serde_json::from_str(&frame.data).unwrap_or(Value::Null)
        };

        match event_type {
            "message_start" => {
                let msg = data.get("message").unwrap_or(&data);
                let id = s(msg, "id");
                let model = s(msg, "model");
                self.header = Some((id.clone(), model.clone()));
                if let Some(u) = msg.get("usage") {
                    self.input_usage = u.clone();
                }
                // Anthropic reports input usage up-front in `message_start`; surface it on the
                // header so a same-dialect re-encode can echo it faithfully.
                let input_usage = if self.input_usage.is_null() {
                    None
                } else {
                    Some(normalize_usage(ProviderId::Anthropic, &self.input_usage))
                };
                out.push(StreamEvent::Created {
                    response: ResponseHeader {
                        id,
                        model,
                        provider: ProviderId::Anthropic,
                        input_usage,
                    },
                });
            }
            "content_block_start" => {
                let block = data.get("content_block").unwrap_or(&data);
                match block.get("type").and_then(|v| v.as_str()).unwrap_or("text") {
                    "tool_use" => {
                        let id = s(block, "id");
                        let name = s(block, "name");
                        self.tools.push(ToolAcc {
                            id: id.clone(),
                            name: name.clone(),
                            arguments: String::new(),
                        });
                        let ti = self.tools.len() - 1;
                        self.cur = Some(CurBlock::Tool(ti));
                        out.push(StreamEvent::OutputItemAdded {
                            index: self.tool_output_index(ti),
                            item: Item::FunctionCall {
                                id,
                                name,
                                arguments: String::new(),
                            },
                        });
                    }
                    _ => {
                        if !self.text_emitted {
                            self.text_emitted = true;
                            out.push(StreamEvent::OutputItemAdded {
                                index: 0,
                                item: Item::Message {
                                    role: Role::Assistant,
                                    content: Content::Text(String::new()),
                                },
                            });
                        }
                        self.cur = Some(CurBlock::Text);
                    }
                }
            }
            "content_block_delta" => {
                let Some(delta) = data.get("delta") else {
                    return out;
                };
                match delta.get("type").and_then(|v| v.as_str()).unwrap_or("") {
                    "text_delta" => {
                        let t = s(delta, "text");
                        if !t.is_empty() {
                            self.text.push_str(&t);
                            out.push(StreamEvent::TextDelta { text: t });
                        }
                    }
                    "input_json_delta" => {
                        if let Some(CurBlock::Tool(ti)) = self.cur {
                            let partial = delta
                                .get("partial_json")
                                .and_then(|v| v.as_str())
                                .unwrap_or("");
                            self.tools[ti].arguments.push_str(partial);
                            out.push(StreamEvent::ToolCallDelta {
                                id: self.tools[ti].id.clone(),
                                name: self.tools[ti].name.clone(),
                                arguments_delta: partial.to_string(),
                            });
                        }
                    }
                    _ => {} // thinking_delta / signature_delta ignored (v1)
                }
            }
            "content_block_stop" => {
                if let Some(CurBlock::Tool(ti)) = self.cur {
                    let t = &self.tools[ti];
                    out.push(StreamEvent::OutputItemDone {
                        index: self.tool_output_index(ti),
                        item: Item::FunctionCall {
                            id: t.id.clone(),
                            name: t.name.clone(),
                            arguments: t.arguments.clone(),
                        },
                    });
                }
                self.cur = None;
            }
            "message_delta" => {
                let delta = data.get("delta").unwrap_or(&data);
                if let Some(sr) = delta.get("stop_reason").and_then(|v| v.as_str()) {
                    self.stop_reason = Some(anthropic_stop_reason(sr));
                }
                if let Some(u) = data.get("usage") {
                    self.out_tokens = n(u, "output_tokens");
                }
            }
            "message_stop" => {
                out.extend(self.finish());
            }
            "error" => {
                // Terminal: prevent finish() from emitting a spurious Completed afterward.
                self.done = true;
                out.push(StreamEvent::Error {
                    error: CoreError::Stream {
                        message: data.to_string(),
                    },
                });
            }
            _ => {} // ping, etc.
        }
        out
    }

    fn finish(&mut self) -> Vec<StreamEvent> {
        if self.done {
            return Vec::new();
        }
        self.done = true;
        let (id, model) = self.header.clone().unwrap_or_default();
        let mut output = Vec::new();
        let text = std::mem::take(&mut self.text);
        if !text.is_empty() {
            output.push(Item::Message {
                role: Role::Assistant,
                content: Content::Text(text),
            });
        }
        for t in std::mem::take(&mut self.tools) {
            output.push(Item::FunctionCall {
                id: t.id,
                name: t.name,
                arguments: t.arguments,
            });
        }
        let stop_reason = self.stop_reason.unwrap_or(StopReason::EndTurn);
        let mut usage = normalize_usage(ProviderId::Anthropic, &self.input_usage);
        usage.output_tokens = self.out_tokens;
        vec![StreamEvent::Completed {
            response: Response {
                id,
                model,
                provider: ProviderId::Anthropic,
                output,
                stop_reason,
                usage,
            },
        }]
    }
}

/// Decode a full Anthropic SSE document into canonical events.
pub fn decode_anthropic(frames: impl IntoIterator<Item = SseFrame>) -> Vec<StreamEvent> {
    decode_all(frames, AnthropicDecoder::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sse::parse_sse;

    fn kinds(events: &[StreamEvent]) -> Vec<&'static str> {
        events
            .iter()
            .map(|e| match e {
                StreamEvent::Created { .. } => "created",
                StreamEvent::OutputItemAdded { .. } => "added",
                StreamEvent::TextDelta { .. } => "text",
                StreamEvent::ToolCallDelta { .. } => "tool",
                StreamEvent::OutputItemDone { .. } => "done",
                StreamEvent::Completed { .. } => "completed",
                StreamEvent::Error { .. } => "error",
            })
            .collect()
    }

    #[test]
    fn cc_text_stream() {
        let sse = concat!(
            "data: {\"id\":\"c1\",\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"Hel\"}}]}\n\n",
            "data: {\"id\":\"c1\",\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"lo\"},\"finish_reason\":\"stop\"}]}\n\n",
            "data: {\"id\":\"c1\",\"model\":\"gpt-4o\",\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":2,\"prompt_tokens_details\":{\"cached_tokens\":4}}}\n\n",
            "data: [DONE]\n\n"
        );
        let ev = decode_cc(ProviderId::Openai, parse_sse(sse));
        assert_eq!(
            kinds(&ev),
            vec!["created", "added", "text", "text", "completed"]
        );
        let completed = match ev.last() {
            Some(StreamEvent::Completed { response }) => response,
            other => panic!("expected Completed, got {other:?}"),
        };
        match &completed.output[0] {
            Item::Message { content, .. } => {
                assert_eq!(content, &Content::Text("Hello".into()));
            }
            other => panic!("expected Message, got {other:?}"),
        }
        assert_eq!(completed.usage.cache_read, 4);
        assert_eq!(completed.usage.input_tokens, 6);
    }

    #[test]
    fn cc_tool_stream_concatenates_arguments() {
        let sse = concat!(
            "data: {\"id\":\"c1\",\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"get_weather\",\"arguments\":\"{\\\"q\\\":\"}}]}}]}\n\n",
            "data: {\"id\":\"c1\",\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"\\\"sf\\\"}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\n",
            "data: [DONE]\n\n"
        );
        let ev = decode_cc(ProviderId::Openai, parse_sse(sse));
        assert!(kinds(&ev).contains(&"tool"));
        let completed = match ev.last() {
            Some(StreamEvent::Completed { response }) => response,
            other => panic!("expected Completed, got {other:?}"),
        };
        assert_eq!(completed.stop_reason, StopReason::ToolUse);
        match &completed.output[0] {
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
    fn cc_done_without_usage_uses_defaults() {
        let sse = "data: {\"id\":\"c1\",\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n";
        let ev = decode_cc(ProviderId::Openai, parse_sse(sse));
        assert!(matches!(ev.last(), Some(StreamEvent::Completed { .. })));
    }

    #[test]
    fn anthropic_text_stream() {
        let sse = concat!(
            "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"model\":\"claude-sonnet-4-6\",\"usage\":{\"input_tokens\":10,\"output_tokens\":1}}}\n\n",
            "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
            "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hel\"}}\n\n",
            "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"lo\"}}\n\n",
            "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
            "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":2}}\n\n",
            "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
        );
        let ev = decode_anthropic(parse_sse(sse));
        assert_eq!(
            kinds(&ev),
            vec!["created", "added", "text", "text", "completed"]
        );
        let completed = match ev.last() {
            Some(StreamEvent::Completed { response }) => response,
            other => panic!("expected Completed, got {other:?}"),
        };
        assert_eq!(completed.stop_reason, StopReason::EndTurn);
        match &completed.output[0] {
            Item::Message { content, .. } => assert_eq!(content, &Content::Text("Hello".into())),
            other => panic!("expected Message, got {other:?}"),
        }
        assert_eq!(completed.usage.output_tokens, 2);
        assert_eq!(completed.usage.input_tokens, 10);
    }

    #[test]
    fn anthropic_tool_stream() {
        let sse = concat!(
            "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_2\",\"model\":\"claude-sonnet-4-6\",\"usage\":{\"input_tokens\":5,\"output_tokens\":1}}}\n\n",
            "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"tool_1\",\"name\":\"get_weather\",\"input\":{}}}\n\n",
            "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"q\\\":\"}}\n\n",
            "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"\\\"sf\\\"}\"}}\n\n",
            "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
            "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":12}}\n\n",
            "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
        );
        let ev = decode_anthropic(parse_sse(sse));
        assert!(kinds(&ev).contains(&"tool"));
        assert!(kinds(&ev).contains(&"done"));
        let completed = match ev.last() {
            Some(StreamEvent::Completed { response }) => response,
            other => panic!("expected Completed, got {other:?}"),
        };
        assert_eq!(completed.stop_reason, StopReason::ToolUse);
        match &completed.output[0] {
            Item::FunctionCall {
                id,
                name,
                arguments,
            } => {
                assert_eq!(id, "tool_1");
                assert_eq!(name, "get_weather");
                assert_eq!(arguments, "{\"q\":\"sf\"}");
            }
            other => panic!("expected FunctionCall, got {other:?}"),
        }
    }

    #[test]
    fn anthropic_error_event() {
        let sse = "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n";
        let ev = decode_anthropic(parse_sse(sse));
        assert!(matches!(ev.last(), Some(StreamEvent::Error { .. })));
    }
}
