//! Outbound streaming: canonical `StreamEvent` → outbound SSE wire lines (`DESIGN.md` §10.5).
//!
//! Mirror of the core decoders (`unillm-core/src/stream_decode.rs`) in reverse: each canonical event
//! is re-translated into the client's outbound wire format and emitted as one or more complete,
//! self-terminated SSE frames. The HTTP handler owns a single [`StreamEncoder`] for the lifetime of a
//! stream and feeds it events as they arrive from the backend — no whole-response buffering.

use std::collections::HashMap;

use serde_json::{Value, json};

use unillm_core::ir::Item;
use unillm_core::stream::StreamEvent;

use crate::inbound::Format;

/// Translate a canonical [`StreamEvent`] into zero or more outbound SSE wire frames.
///
/// Each returned `String` is a complete SSE frame (`data: …\n\n`, or — for Anthropic —
/// `event: …\ndata: …\n\n`), ready to flush verbatim to the client.
pub trait StreamEncoder: Send {
    fn encode_event(&mut self, event: &StreamEvent) -> Vec<String>;
}

/// Select the encoder for an outbound [`Format`] (`DESIGN.md` §10.4, §10.5).
pub fn encoder_for(format: Format) -> Box<dyn StreamEncoder> {
    match format {
        Format::OpenaiChat => Box::new(CcStreamEncoder::new()),
        Format::Anthropic => Box::new(AnthropicStreamEncoder::new()),
        // Canonical outbound: each event is serialized verbatim (it is already internally tagged).
        Format::Unillm => Box::new(UnillmStreamEncoder),
    }
}

/// A `data:`-only SSE frame (CC chunks and the canonical/unillm stream use no `event:` line).
fn data_frame(v: &Value) -> String {
    format!("data: {v}\n\n")
}

/// An Anthropic `event:` + `data:` frame.
fn event_frame(event: &str, v: &Value) -> String {
    format!("event: {event}\ndata: {v}\n\n")
}

const DONE: &str = "data: [DONE]\n\n";

// -------------------------------------------------------------------------------------------------
// Chat Completions encoder (DESIGN.md §6.3, inverse)
// -------------------------------------------------------------------------------------------------

/// Emits OpenAI Chat Completions streaming chunks (`object: "chat.completion.chunk"`), ending with
/// `data: [DONE]`.
struct CcStreamEncoder {
    id: String,
    model: String,
    /// Canonical tool-call `id` → CC `tool_calls[].index` (CC numbers tool calls positionally).
    tools: HashMap<String, usize>,
    next_tool_index: usize,
    done: bool,
}

impl CcStreamEncoder {
    fn new() -> Self {
        Self {
            id: String::new(),
            model: String::new(),
            tools: HashMap::new(),
            next_tool_index: 0,
            done: false,
        }
    }

    /// A chunk carrying `delta` and (optionally) a terminal `finish_reason`, plus the stream header.
    fn chunk(&self, delta: Value, finish_reason: Option<&str>) -> Value {
        let mut choices = json!([{ "index": 0, "delta": delta }]);
        choices[0]["finish_reason"] = match finish_reason {
            Some(fr) => json!(fr),
            None => Value::Null,
        };
        json!({
            "id": self.id,
            "object": "chat.completion.chunk",
            // Canonical `Response` carries no timestamp; `created` is not meaningful (see outbound/cc.rs).
            "created": 0,
            "model": self.model,
            "choices": choices,
        })
    }

    /// The CC positional index for a tool-call id, assigning the next one on first sight.
    fn tool_index(&mut self, id: &str) -> usize {
        if let Some(&i) = self.tools.get(id) {
            i
        } else {
            let i = self.next_tool_index;
            self.next_tool_index += 1;
            self.tools.insert(id.to_string(), i);
            i
        }
    }
}

impl StreamEncoder for CcStreamEncoder {
    fn encode_event(&mut self, event: &StreamEvent) -> Vec<String> {
        if self.done {
            return Vec::new();
        }
        let mut out = Vec::new();
        match event {
            StreamEvent::Created { response } => {
                self.id = response.id.clone();
                self.model = response.model.clone();
                out.push(data_frame(
                    &self.chunk(json!({ "role": "assistant" }), None),
                ));
            }
            StreamEvent::OutputItemAdded { item, .. } => {
                if let Item::FunctionCall { id, name, .. } = item {
                    let idx = self.tool_index(id);
                    out.push(data_frame(&self.chunk(
                        json!({
                            "tool_calls": [{
                                "index": idx,
                                "id": id,
                                "type": "function",
                                "function": { "name": name, "arguments": "" }
                            }]
                        }),
                        None,
                    )));
                }
                // An assistant-message add is implicit: the role left in `Created`, text arrives as deltas.
            }
            StreamEvent::TextDelta { text } => {
                out.push(data_frame(&self.chunk(json!({ "content": text }), None)));
            }
            StreamEvent::ToolCallDelta {
                id,
                arguments_delta,
                ..
            } => {
                let idx = self.tool_index(id);
                out.push(data_frame(&self.chunk(
                    json!({
                        "tool_calls": [{ "index": idx, "function": { "arguments": arguments_delta } }]
                    }),
                    None,
                )));
            }
            StreamEvent::OutputItemDone { .. } => {} // CC has no per-item done signal.
            StreamEvent::Completed { response } => {
                let mut chunk = self.chunk(
                    json!({}),
                    Some(super::cc::cc_finish_reason(response.stop_reason)),
                );
                chunk["usage"] = super::cc::cc_usage(&response.usage);
                out.push(data_frame(&chunk));
                out.push(DONE.to_string());
                self.done = true;
            }
            StreamEvent::Error { .. } => {
                // CC defines no in-stream error frame; terminate gracefully with `[DONE]`.
                out.push(DONE.to_string());
                self.done = true;
            }
        }
        out
    }
}

// -------------------------------------------------------------------------------------------------
// Anthropic Messages encoder (DESIGN.md §6.4, inverse)
// -------------------------------------------------------------------------------------------------

/// Emits the Anthropic SSE lifecycle (`message_start` → `content_block_*` → `message_delta` →
/// `message_stop`).
struct AnthropicStreamEncoder {
    id: String,
    model: String,
    /// Index of the content block currently being streamed (set on `*_start`, cleared on `_stop`).
    cur_block: Option<u32>,
    done: bool,
}

impl AnthropicStreamEncoder {
    fn new() -> Self {
        Self {
            id: String::new(),
            model: String::new(),
            cur_block: None,
            done: false,
        }
    }
}

impl StreamEncoder for AnthropicStreamEncoder {
    fn encode_event(&mut self, event: &StreamEvent) -> Vec<String> {
        if self.done {
            return Vec::new();
        }
        let mut out = Vec::new();
        match event {
            StreamEvent::Created { response } => {
                self.id = response.id.clone();
                self.model = response.model.clone();
                // Echo any input usage the backend reported at open (Anthropic `message_start`).
                // When none is known yet (CC dialect), emit zeros; the authoritative counts arrive
                // in `message_delta` from `Completed`. Output is unknown at open in either case.
                let (input, cache_read, cache_creation) = match &response.input_usage {
                    Some(u) => (u.input_tokens, u.cache_read, u.cache_creation),
                    None => (0, 0, 0),
                };
                out.push(event_frame(
                    "message_start",
                    &json!({
                        "type": "message_start",
                        "message": {
                            "id": self.id,
                            "type": "message",
                            "role": "assistant",
                            "model": self.model,
                            "content": [],
                            "stop_reason": Value::Null,
                            "stop_sequence": Value::Null,
                            "usage": {
                                "input_tokens": input,
                                "cache_read_input_tokens": cache_read,
                                "cache_creation_input_tokens": cache_creation,
                                "output_tokens": 0,
                            }
                        }
                    }),
                ));
            }
            StreamEvent::OutputItemAdded { index, item } => {
                self.cur_block = Some(*index);
                let block = match item {
                    Item::FunctionCall { id, name, .. } => {
                        json!({ "type": "tool_use", "id": id, "name": name, "input": {} })
                    }
                    // Any message (or other) item opens a text block.
                    _ => json!({ "type": "text", "text": "" }),
                };
                out.push(event_frame(
                    "content_block_start",
                    &json!({
                        "type": "content_block_start",
                        "index": index,
                        "content_block": block
                    }),
                ));
            }
            StreamEvent::TextDelta { text } => {
                let idx = self.cur_block.unwrap_or(0);
                out.push(event_frame(
                    "content_block_delta",
                    &json!({
                        "type": "content_block_delta",
                        "index": idx,
                        "delta": { "type": "text_delta", "text": text }
                    }),
                ));
            }
            StreamEvent::ToolCallDelta {
                arguments_delta, ..
            } => {
                let idx = self.cur_block.unwrap_or(0);
                out.push(event_frame(
                    "content_block_delta",
                    &json!({
                        "type": "content_block_delta",
                        "index": idx,
                        "delta": { "type": "input_json_delta", "partial_json": arguments_delta }
                    }),
                ));
            }
            StreamEvent::OutputItemDone { index, .. } => {
                out.push(event_frame(
                    "content_block_stop",
                    &json!({ "type": "content_block_stop", "index": index }),
                ));
                self.cur_block = None;
            }
            StreamEvent::Completed { response } => {
                out.push(event_frame(
                    "message_delta",
                    &json!({
                        "type": "message_delta",
                        "delta": {
                            "stop_reason": super::anthropic::anthropic_stop_reason(response.stop_reason),
                            "stop_sequence": Value::Null
                        },
                        "usage": super::anthropic::anthropic_usage(&response.usage),
                    }),
                ));
                out.push(event_frame(
                    "message_stop",
                    &json!({ "type": "message_stop" }),
                ));
                self.done = true;
            }
            StreamEvent::Error { error } => {
                out.push(event_frame(
                    "error",
                    &json!({
                        "type": "error",
                        "error": { "type": "api_error", "message": error.to_string() }
                    }),
                ));
                self.done = true;
            }
        }
        out
    }
}

// -------------------------------------------------------------------------------------------------
// Canonical/unillm encoder
// -------------------------------------------------------------------------------------------------

/// Emits each canonical [`StreamEvent`] verbatim — the wire format *is* canonical (`DESIGN.md` §10.1).
struct UnillmStreamEncoder;

impl StreamEncoder for UnillmStreamEncoder {
    fn encode_event(&mut self, event: &StreamEvent) -> Vec<String> {
        match serde_json::to_value(event) {
            Ok(v) => vec![data_frame(&v)],
            Err(_) => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use unillm_core::ProviderId;
    use unillm_core::ir::{Content, Response, Role, StopReason, Usage};
    use unillm_core::stream::ResponseHeader;

    fn header() -> ResponseHeader {
        ResponseHeader {
            id: "c1".into(),
            model: "gpt-4o".into(),
            provider: ProviderId::Openai,
            input_usage: None,
        }
    }

    fn completed() -> Response {
        Response {
            id: "c1".into(),
            model: "gpt-4o".into(),
            provider: ProviderId::Openai,
            output: vec![],
            stop_reason: StopReason::EndTurn,
            usage: Usage {
                input_tokens: 6,
                output_tokens: 2,
                cache_read: 4,
                cache_creation: 0,
                cost_usd: None,
            },
        }
    }

    #[test]
    fn cc_encodes_full_text_stream() {
        let mut enc = CcStreamEncoder::new();
        let frames = enc.encode_event(&StreamEvent::Created { response: header() });
        assert_eq!(frames.len(), 1);
        assert!(frames[0].contains("\"role\":\"assistant\""));
        assert!(frames[0].contains("chat.completion.chunk"));

        let frames = enc.encode_event(&StreamEvent::TextDelta { text: "Hi".into() });
        assert_eq!(frames.len(), 1);
        assert!(frames[0].contains("\"content\":\"Hi\""));

        let frames = enc.encode_event(&StreamEvent::Completed {
            response: completed(),
        });
        // terminal chunk + [DONE]
        assert_eq!(frames.len(), 2);
        assert!(frames[0].contains("\"finish_reason\":\"stop\""));
        assert!(frames[0].contains("\"prompt_tokens\":10")); // input(6) + cache_read(4)
        assert_eq!(frames[1], DONE);

        // Once done, no further frames.
        assert!(
            enc.encode_event(&StreamEvent::TextDelta { text: "x".into() })
                .is_empty()
        );
    }

    #[test]
    fn anthropic_encodes_full_text_stream() {
        let mut enc = AnthropicStreamEncoder::new();
        // `message_start` echoes the input usage the backend reported at open.
        let hdr = ResponseHeader {
            id: "msg_1".into(),
            model: "claude".into(),
            provider: ProviderId::Anthropic,
            input_usage: Some(Usage {
                input_tokens: 10,
                output_tokens: 0,
                cache_read: 2,
                cache_creation: 1,
                cost_usd: None,
            }),
        };
        let frames = enc.encode_event(&StreamEvent::Created { response: hdr });
        assert_eq!(frames.len(), 1);
        assert!(frames[0].contains("event: message_start"));
        assert!(frames[0].contains("\"input_tokens\":10"));
        assert!(frames[0].contains("\"cache_read_input_tokens\":2"));

        let msg = Item::Message {
            role: Role::Assistant,
            content: Content::Text(String::new()),
        };
        let frames = enc.encode_event(&StreamEvent::OutputItemAdded {
            index: 0,
            item: msg.clone(),
        });
        assert_eq!(frames.len(), 1);
        assert!(frames[0].starts_with("event: content_block_start"));

        let frames = enc.encode_event(&StreamEvent::TextDelta { text: "Hi".into() });
        assert_eq!(frames.len(), 1);
        assert!(frames[0].contains("text_delta"));
        assert!(frames[0].contains("\"text\":\"Hi\""));

        enc.encode_event(&StreamEvent::OutputItemDone {
            index: 0,
            item: msg,
        });

        let frames = enc.encode_event(&StreamEvent::Completed {
            response: completed(),
        });
        // message_delta + message_stop
        assert_eq!(frames.len(), 2);
        assert!(frames[0].starts_with("event: message_delta"));
        assert!(frames[0].contains("\"stop_reason\":\"end_turn\""));
        assert_eq!(
            frames[1],
            "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
        );
    }

    #[test]
    fn unillm_encodes_canonical_events_verbatim() {
        let mut enc = UnillmStreamEncoder;
        let frames = enc.encode_event(&StreamEvent::Created { response: header() });
        assert_eq!(frames.len(), 1);
        assert!(frames[0].contains("\"type\":\"created\""));
        assert!(frames[0].contains("\"id\":\"c1\""));
        assert!(frames[0].ends_with("\n\n"));
    }
}
