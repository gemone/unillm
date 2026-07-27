//! Proxy middleware (`DESIGN.md` §10.3 pipeline).
//!
//! Implemented as inline stages over the request (not axum layers) so they can interact with the
//! streaming commit-at-first-event semantics from M3.4. M4.2 ships auth; M4.4 ships rate-limit;
//! M4.5 ships request/usage logging; M5.1 ships the exact-hash response cache.

pub mod auth;
pub mod cache;
pub mod log;
pub mod rate_limit;
