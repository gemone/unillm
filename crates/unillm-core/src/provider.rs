//! Provider abstraction and configuration (`DESIGN.md` §5.1, §5.6).
//!
//! A [`Provider`] is a pure dialect transform: it turns a canonical [`Request`] into a provider's
//! native JSON payload and parses a native response back into a canonical [`Response``. The actual
//! HTTP execution lives in the transport layer (`http.rs`, added in M1.5) so the transforms remain
//! trivially testable without any network.

use std::collections::HashMap;
use std::time::Duration;

use serde_json::Value;

use crate::error::CoreError;
use crate::ir::{ModelRef, ProviderId, Request, Response, StopReason};

/// A provider wire-format family (`DESIGN.md` §2.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    /// `POST /chat/completions` — OpenAI, DeepSeek, OpenRouter.
    ChatCompletions,
    /// `POST /messages` — Anthropic.
    Anthropic,
    /// `POST /responses` — OpenAI (fast-follow; out of v1).
    Responses,
}

/// Everything needed to construct and address a provider (`DESIGN.md` §5.6).
#[derive(Debug, Clone)]
pub struct ProviderConfig {
    pub provider: ProviderId,
    pub base_url: String,
    pub api_key: String,
    pub dialect: Dialect,
    /// Headers applied to every request (e.g. `anthropic-version`, OpenRouter `X-Title`).
    pub default_headers: HashMap<String, String>,
    pub request_timeout: Option<Duration>,
}

impl ProviderConfig {
    /// Pick the natural base URL, dialect, and mandatory headers for a provider (`DESIGN.md` §5.6).
    pub fn new(provider: ProviderId, api_key: impl Into<String>) -> Self {
        let dialect = match provider {
            ProviderId::Openai | ProviderId::Openrouter | ProviderId::Deepseek => {
                Dialect::ChatCompletions
            }
            ProviderId::Anthropic => Dialect::Anthropic,
        };
        let mut default_headers = HashMap::new();
        if provider == ProviderId::Anthropic {
            default_headers.insert("anthropic-version".to_string(), "2023-06-01".to_string());
        }
        Self {
            provider,
            base_url: default_base_url(provider).to_string(),
            api_key: api_key.into(),
            dialect,
            default_headers,
            request_timeout: None,
        }
    }
}

/// The default API base URL for a provider (`DESIGN.md` §5.6). Single source of truth — shared by
/// [`ProviderConfig::new`] and the proxy's env loader so the endpoints live in exactly one place.
pub fn default_base_url(provider: ProviderId) -> &'static str {
    match provider {
        ProviderId::Openai => "https://api.openai.com/v1",
        ProviderId::Anthropic => "https://api.anthropic.com/v1",
        ProviderId::Openrouter => "https://openrouter.ai/api/v1",
        ProviderId::Deepseek => "https://api.deepseek.com",
    }
}

/// A pure dialect transform (`DESIGN.md` §5.1).
///
/// `build_payload` / `parse_response` are synchronous and side-effect-free. Streaming decode is
/// handled by the per-dialect decoders (`stream_decode`); HTTP transport is layered on top.
pub trait Provider: Send + Sync {
    fn provider_id(&self) -> ProviderId;
    fn dialect(&self) -> Dialect;
    fn build_payload(&self, req: &Request) -> Value;
    fn parse_response(&self, body: &Value) -> Result<Response, CoreError>;
}

/// The wire model string for a [`ModelRef`] — the alias, or the explicit pair's model (`DESIGN.md` §4.1).
pub(crate) fn model_string(m: &ModelRef) -> String {
    match m {
        ModelRef::Alias(s) => s.clone(),
        ModelRef::Explicit { model, .. } => model.clone(),
    }
}

/// Format an `f32` cleanly as a JSON number. Storing it in a `serde_json::Value` goes via `f64`,
/// which injects representation noise (e.g. `0.7f32` → `0.699999988079071`). Round-tripping through
/// the shortest `f32` string keeps provider payloads tidy.
pub(crate) fn f32_to_value(x: f32) -> Value {
    match format!("{x}").parse::<f64>() {
        Ok(n) => Value::from(n),
        Err(_) => Value::Null,
    }
}

/// Map a Chat Completions `finish_reason` onto a canonical `StopReason` (`DESIGN.md` §5.4).
pub(crate) fn cc_finish_to_stop_reason(s: &str) -> StopReason {
    match s {
        "stop" => StopReason::EndTurn,
        "length" => StopReason::MaxTokens,
        "tool_calls" | "function_call" => StopReason::ToolUse,
        "stop_sequence" => StopReason::StopSequence,
        _ => StopReason::Other,
    }
}

/// Map an Anthropic `stop_reason` onto a canonical `StopReason` (`DESIGN.md` §5.4).
pub(crate) fn anthropic_stop_reason(s: &str) -> StopReason {
    match s {
        "end_turn" => StopReason::EndTurn,
        "max_tokens" => StopReason::MaxTokens,
        "stop_sequence" => StopReason::StopSequence,
        "tool_use" => StopReason::ToolUse,
        "refusal" => StopReason::Refusal,
        "pause_turn" => StopReason::Paused,
        _ => StopReason::Other,
    }
}
