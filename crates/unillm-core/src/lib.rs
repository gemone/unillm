//! unillm-core: the canonical IR, error model, provider adapters, SSE codec, and cache logic.
//!
//! Shared by the Python SDK and the proxy so all normalization lives in exactly one place
//! (`DESIGN.md` §3). This milestone (M0) implements the canonical data model (`DESIGN.md` §4) and
//! error catalog (`DESIGN.md` §15); provider adapters, the SSE codec, and cache logic arrive in M1.

pub mod error;
pub mod ir;
pub mod stream;

pub use error::CoreError;
pub use ir::{
    Breakpoint, CacheControl, CacheStrategy, Content, ContentBlock, ImageSource, Item, ModelRef,
    ProviderId, Request, Response, Role, StopReason, ToolChoice, ToolDef, Ttl, Usage,
};
pub use stream::{ResponseHeader, StreamEvent};
