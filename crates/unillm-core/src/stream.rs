//! Streaming events (`DESIGN.md` §4.9).
//!
//! `StreamEvent` is the incremental projection of a `Response`: providers' native SSE events are
//! decoded into this taxonomy (see §6) and may be re-translated into any client's outbound format
//! by the proxy.

use serde::{Deserialize, Serialize};

use crate::error::CoreError;
use crate::{Item, ProviderId, Response, Usage};

/// Minimal response header emitted at stream creation (`DESIGN.md` §4.9).
///
/// `Eq` is intentionally NOT derived: `input_usage` carries `Usage`, which holds an `f64` cost.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResponseHeader {
    pub id: String,
    pub model: String,
    pub provider: ProviderId,
    /// Input/prompt usage known at stream open, when the backend reports it early (Anthropic's
    /// `message_start`). `None` for backends that report usage only at completion (CC dialect).
    /// Output token counts are unknown at open; the authoritative totals arrive in `Completed`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_usage: Option<Usage>,
}

/// A canonical streaming event (`DESIGN.md` §4.9). Internally tagged by `type`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamEvent {
    /// Stream opened; carries the response id/model/provider.
    Created { response: ResponseHeader },
    /// A new output item began streaming (index within the response's `output`).
    OutputItemAdded { index: u32, item: Item },
    /// Incremental assistant text.
    TextDelta { text: String },
    /// Incremental chain-of-thought from a reasoning model (e.g. DeepSeek `reasoning_content`).
    /// Consumers concatenate `text` to reconstruct the full reasoning, which is also carried as an
    /// `Item::Reasoning` in the terminal `Completed` response.
    ReasoningDelta { text: String },
    /// Incremental tool-call arguments. Consumers concatenate `arguments_delta` to reconstruct the
    /// full JSON `arguments` string (`DESIGN.md` §8.2).
    ToolCallDelta {
        id: String,
        name: String,
        arguments_delta: String,
    },
    /// An output item finished streaming.
    OutputItemDone { index: u32, item: Item },
    /// Stream completed; carries the full `Response` including usage.
    Completed { response: Response },
    /// A terminal error mid-stream.
    Error { error: CoreError },
}
