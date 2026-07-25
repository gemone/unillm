//! Canonical intermediate representation (`DESIGN.md` §4).
//!
//! This is the single normalized data model every provider adapter maps to and from. Provider-native
//! shapes never leak past these types. `Request` and `Response` share the same `Item` / `Content` /
//! `ContentBlock` vocabulary, so a response can be fed straight back into the next request's `input`
//! for multi-turn conversations (`DESIGN.md` §8).

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

// -------------------------------------------------------------------------------------------------
// Identifiers
// -------------------------------------------------------------------------------------------------

/// A backend provider (`DESIGN.md` §4.6).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderId {
    Openai,
    Anthropic,
    Openrouter,
    Deepseek,
}

/// A model reference: either a routing alias or an explicit provider+model pair (`DESIGN.md` §4.1).
///
/// Serialized untagged so an alias is a bare JSON string (`"claude-sonnet-4-6"`) and an explicit
/// pair is an object (`{"provider":"openai","model":"gpt-4o"}`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ModelRef {
    Alias(String),
    Explicit { provider: ProviderId, model: String },
}

/// The conversational role of a message (`DESIGN.md` §4.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    System,
    User,
    Assistant,
    Tool,
}

// -------------------------------------------------------------------------------------------------
// Cache control
// -------------------------------------------------------------------------------------------------

/// Prompt-cache time-to-live. Anthropic values (`DESIGN.md` §4.8).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum Ttl {
    #[default]
    #[serde(rename = "5m")]
    FiveMinutes,
    #[serde(rename = "1h")]
    OneHour,
}

/// Wire marker instructing a provider to cache up to a content boundary (`DESIGN.md` §4.8).
///
/// Modeled as a single-variant tagged enum so the wire shape is exactly
/// `{"type":"ephemeral","ttl"?}` and remains extensible if Anthropic adds more control kinds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CacheControl {
    Ephemeral {
        #[serde(skip_serializing_if = "Option::is_none")]
        ttl: Option<Ttl>,
    },
}

/// A cache-control insertion point (`DESIGN.md` §4.8).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "at", rename_all = "snake_case")]
pub enum Breakpoint {
    Instructions,
    Message { index: u32 },
    Last,
}

/// How cache-control should be applied to a request (`DESIGN.md` §4.8).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CacheStrategy {
    /// Rely on the provider's automatic prefix cache; normalize usage on the way back. (Default.)
    #[default]
    Auto,
    /// Inject explicit `cache_control` breakpoints. Honored by Anthropic; debug-logged elsewhere.
    Explicit {
        breakpoints: Vec<Breakpoint>,
        #[serde(default)]
        ttl: Ttl,
    },
    /// Opt out of caching where the provider allows.
    None,
}

// -------------------------------------------------------------------------------------------------
// Content
// -------------------------------------------------------------------------------------------------

/// An image source, by URL or inline base64 (`DESIGN.md` §4.3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ImageSource {
    Url { url: String },
    Base64 { media_type: String, data: String },
}

/// Message content: either a plain string or a sequence of typed blocks (`DESIGN.md` §4.3).
///
/// Serialized untagged so plain text is a bare JSON string and structured content is a JSON array.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Content {
    Text(String),
    Blocks(Vec<ContentBlock>),
}

/// A typed content block within a message (`DESIGN.md` §4.3).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlock {
    Text {
        text: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        cache_control: Option<CacheControl>,
    },
    Image {
        source: ImageSource,
        #[serde(skip_serializing_if = "Option::is_none")]
        cache_control: Option<CacheControl>,
    },
    /// A tool invocation emitted by the model. `input` is a parsed JSON object (canonical); adapters
    /// stringify it on the way out to the provider (`DESIGN.md` §4.3, §5.3).
    ToolUse {
        id: String,
        name: String,
        input: Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        cache_control: Option<CacheControl>,
    },
    /// A tool result fed back to the model. `content` may itself be text or structured blocks.
    ToolResult {
        tool_use_id: String,
        content: Content,
        #[serde(skip_serializing_if = "Option::is_none")]
        cache_control: Option<CacheControl>,
    },
}

// -------------------------------------------------------------------------------------------------
// Items, tools
// -------------------------------------------------------------------------------------------------

/// A typed input/output item (`DESIGN.md` §4.2).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Item {
    Message {
        role: Role,
        content: Content,
    },
    /// Model reasoning. `encrypted` carries opaque state for stateless replay (Responses dialect).
    Reasoning {
        summary: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        encrypted: Option<String>,
    },
    /// A tool the model wants to call. `arguments` is a JSON **string**, matching provider wire
    /// format and letting adapters pass it through verbatim (`DESIGN.md` §4.2, §5.4).
    FunctionCall {
        id: String,
        name: String,
        arguments: String,
    },
    /// A tool result fed back; `call_id` correlates with `FunctionCall.id` (`DESIGN.md` §8.2).
    FunctionCallOutput {
        call_id: String,
        output: String,
    },
}

/// A tool/function definition (`DESIGN.md` §4.4).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolDef {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// A JSON Schema object describing the function parameters.
    pub input_schema: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_control: Option<CacheControl>,
}

/// How the model should choose tools (`DESIGN.md` §4.5). Internally tagged by `type`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ToolChoice {
    Auto,
    None,
    Required,
    Named { name: String },
}

// -------------------------------------------------------------------------------------------------
// Request / Response / Usage
// -------------------------------------------------------------------------------------------------

/// `true` predicate for `#[serde(skip_serializing_if)]` on `bool` fields defaulting to `false`.
fn is_false(b: &bool) -> bool {
    !*b
}

/// A canonical request (`DESIGN.md` §4.1).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Request {
    pub model: ModelRef,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
    #[serde(default)]
    pub input: Vec<Item>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<ToolDef>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<ToolChoice>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub stream: bool,
    #[serde(default)]
    pub cache: CacheStrategy,
    /// Caller-extensible metadata (request id, user id, tags). **Not** forwarded to the provider
    /// unless an adapter explicitly opts in (`DESIGN.md` §4.1).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, Value>,
}

/// Why generation stopped (`DESIGN.md` §4.6).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    EndTurn,
    MaxTokens,
    StopSequence,
    ToolUse,
    Refusal,
    Paused,
    Other,
}

/// Token and cost accounting (`DESIGN.md` §4.7).
///
/// Invariant: `input_tokens + cache_read + cache_creation` equals the provider's total prompt
/// tokens. `input_tokens` is the **non-cached** input count.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Usage {
    /// Non-cached input tokens.
    pub input_tokens: u64,
    pub output_tokens: u64,
    /// Input tokens served from the provider's prompt cache.
    pub cache_read: u64,
    /// Input tokens written to the provider's cache this request.
    pub cache_creation: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost_usd: Option<f64>,
}

impl Usage {
    /// Total prompt tokens as the provider bills them: non-cached input + cache reads + cache writes.
    pub fn total_input(&self) -> u64 {
        self.input_tokens + self.cache_read + self.cache_creation
    }

    /// Zero usage, e.g. for a cache hit that served no upstream call.
    pub fn zero() -> Self {
        Self {
            input_tokens: 0,
            output_tokens: 0,
            cache_read: 0,
            cache_creation: 0,
            cost_usd: None,
        }
    }
}

impl Default for Usage {
    fn default() -> Self {
        Self::zero()
    }
}

/// A canonical response (`DESIGN.md` §4.6).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Response {
    pub id: String,
    pub model: String,
    pub provider: ProviderId,
    pub output: Vec<Item>,
    pub stop_reason: StopReason,
    pub usage: Usage,
}
