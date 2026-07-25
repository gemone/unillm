//! unillm-proxy: the universal bidirectional translator (`DESIGN.md` §10).
//!
//! Accepts any inbound client format (OpenAI Chat Completions, Anthropic Messages, or canonical),
//! normalizes to the canonical IR, routes to a backend via [`unillm_core`], and returns any outbound
//! format. This slice (M3.1) provides the inbound transforms; the axum server, routing, outbound
//! transforms, and middleware arrive in later M3 slices.

pub mod inbound;
pub mod outbound;

pub use inbound::{Format, detect_format, parse_request};
pub use outbound::build_response;
