//! Inbound format detection + parsing (`DESIGN.md` §10.1).

use serde_json::Value;
use unillm_core::{CoreError, Request};

pub mod anthropic;
pub mod cc;

/// The inbound wire format of a client request (`DESIGN.md` §10.1, §2.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    /// `POST /v1/chat/completions` — OpenAI / DeepSeek / OpenRouter.
    OpenaiChat,
    /// `POST /v1/messages` — Anthropic.
    Anthropic,
    /// Canonical unillm `Request` (`POST /unillm/v1/responses`).
    Unillm,
}

/// Detect the inbound format (`DESIGN.md` §10.1): path prefix → `X-Unillm-Format` header → body shape.
pub fn detect_format(path: &str, header: Option<&str>, body: &Value) -> Format {
    if path.contains("/chat/completions") {
        return Format::OpenaiChat;
    }
    if path.contains("/messages") {
        return Format::Anthropic;
    }
    if path.contains("/unillm/") {
        return Format::Unillm;
    }
    if let Some(h) = header {
        return match h {
            "openai_chat" => Format::OpenaiChat,
            "anthropic" => Format::Anthropic,
            _ => Format::Unillm,
        };
    }
    // Body auto-detect: Anthropic carries a top-level `system`; CC/unillm do not. Unillm uses `input`.
    if body.get("messages").is_some() {
        if body.get("system").is_some() {
            Format::Anthropic
        } else {
            Format::OpenaiChat
        }
    } else if body.get("input").is_some() {
        Format::Unillm
    } else {
        Format::OpenaiChat
    }
}

/// Parse an inbound body into a canonical [`Request`] (`DESIGN.md` §10.3, step 2).
pub fn parse_request(format: Format, body: &Value) -> Result<Request, CoreError> {
    match format {
        Format::OpenaiChat => cc::parse_cc_request(body),
        Format::Anthropic => anthropic::parse_anthropic_request(body),
        Format::Unillm => serde_json::from_value(body.clone()).map_err(|e| CoreError::Serde {
            message: format!("invalid canonical request: {e}"),
        }),
    }
}

/// Read a string field, defaulting to `""` (shared by the dialect parsers).
pub(super) fn s(v: &Value, key: &str) -> String {
    v.get(key)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}

/// Extract the sampling params shared by the CC and Anthropic request shapes (`max_tokens`,
/// `temperature`, `top_p`, `stream` — identical field names in both).
pub(super) fn sampling(body: &Value) -> (Option<u32>, Option<f32>, Option<f32>, bool) {
    (
        body.get("max_tokens")
            .and_then(|v| v.as_u64())
            .map(|x| x as u32),
        body.get("temperature")
            .and_then(|v| v.as_f64())
            .map(|x| x as f32),
        body.get("top_p").and_then(|v| v.as_f64()).map(|x| x as f32),
        body.get("stream")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn detect_by_path() {
        assert_eq!(
            detect_format("/v1/chat/completions", None, &json!({})),
            Format::OpenaiChat
        );
        assert_eq!(
            detect_format("/v1/messages", None, &json!({})),
            Format::Anthropic
        );
        assert_eq!(
            detect_format("/unillm/v1/responses", None, &json!({})),
            Format::Unillm
        );
    }

    #[test]
    fn detect_by_header() {
        assert_eq!(
            detect_format("/", Some("anthropic"), &json!({})),
            Format::Anthropic
        );
        assert_eq!(
            detect_format("/", Some("openai_chat"), &json!({})),
            Format::OpenaiChat
        );
    }

    #[test]
    fn detect_by_body() {
        assert_eq!(
            detect_format("/", None, &json!({"messages": [], "system": "s"})),
            Format::Anthropic
        );
        assert_eq!(
            detect_format("/", None, &json!({"messages": []})),
            Format::OpenaiChat
        );
        assert_eq!(
            detect_format("/", None, &json!({"input": []})),
            Format::Unillm
        );
    }
}
