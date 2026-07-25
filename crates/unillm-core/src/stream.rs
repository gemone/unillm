//! Streaming events (`DESIGN.md` §4.9).
//!
//! `StreamEvent` is the incremental projection of a `Response`: providers' native SSE events are
//! decoded into this taxonomy (see §6) and may be re-translated into any client's outbound format
//! by the proxy.

use serde::{Deserialize, Serialize};

use crate::error::CoreError;
use crate::{Item, ProviderId, Response};

/// Minimal response header emitted at stream creation (`DESIGN.md` §4.9).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResponseHeader {
    pub id: String,
    pub model: String,
    pub provider: ProviderId,
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
