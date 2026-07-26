//! Proxy middleware (`DESIGN.md` §10.3 pipeline).
//!
//! Implemented as inline stages over the request (not axum layers) so they can interact with the
//! streaming commit-at-first-event semantics from M3.4. M4.2 ships auth; M4.4 ships rate-limit;
//! request/usage logging (M4.5) arrives later.

pub mod auth;
pub mod rate_limit;
