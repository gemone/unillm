//! Proxy middleware (`DESIGN.md` §10.3 pipeline).
//!
//! Implemented as inline stages over the request (not axum layers) so they can interact with the
//! streaming commit-at-first-event semantics from M3.4. M4.2 ships auth; rate-limit (M4.4) and
//! request/usage logging (M4.5) arrive later.

pub mod auth;
