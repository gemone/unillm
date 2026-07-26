//! unillm-proxy: the universal bidirectional translator (`DESIGN.md` §10).
//!
//! Accepts any inbound client format (OpenAI Chat Completions, Anthropic Messages, or canonical),
//! normalizes to the canonical IR, routes to a backend via [`unillm_core`], and returns any outbound
//! format. In-memory only (no DB/Redis/keys/RL — M4).

pub mod admin;
pub mod cli;
pub mod config;
pub mod inbound;
pub mod middleware;
pub mod outbound;
pub mod route;
pub mod server;

pub use inbound::{Format, detect_format, parse_request};
pub use outbound::build_response;
pub use route::{RouteTarget, row_to_chain};
pub use server::{AppState, build_app};
