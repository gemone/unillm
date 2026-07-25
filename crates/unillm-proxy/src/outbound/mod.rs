//! Outbound: canonical `Response` → native response body (inverse of `parse_response`, §5.4).
//!
//! The proxy normalizes a backend response to canonical, then re-translates it into whatever
//! outbound format the client asked for (`DESIGN.md` §10.4).

use serde_json::Value;

use unillm_core::Response;

use crate::inbound::Format;

pub mod anthropic;
pub mod cc;

/// Build a native response body in the requested format (`DESIGN.md` §10.4).
pub fn build_response(format: Format, resp: &Response) -> Value {
    match format {
        Format::OpenaiChat => cc::build_cc_response(resp),
        Format::Anthropic => anthropic::build_anthropic_response(resp),
        // Canonical outbound is just the serialized `Response`.
        Format::Unillm => serde_json::to_value(resp).unwrap_or(Value::Null),
    }
}
