//! Rate-limit response helpers + the stream-release guard (`DESIGN.md` §12).

use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use serde_json::json;
use uuid::Uuid;

use unillm_storage::{KeyLimits, RateHeaders, RateLimiter, TokenEstimate};

/// A crude pre-call token estimate (`DESIGN.md` §12.1): ~4 bytes/token for the prompt, plus the
/// requested `max_tokens` (or a default). Only used when the key has token-based limits.
pub fn estimate_tokens(body_len: usize, max_tokens: Option<u32>) -> TokenEstimate {
    TokenEstimate {
        prompt: (body_len / 4) as u64,
        max_output: max_tokens.map(|m| m as u64).unwrap_or(1024),
    }
}

/// Set the `X-Unillm-RateLimit-*` headers (`DESIGN.md` §12.3) on a response.
pub fn apply_rate_headers(headers: &mut HeaderMap, rate: &RateHeaders) {
    set_numeric_header(headers, "x-unillm-ratelimit-limit", rate.limit);
    set_numeric_header(headers, "x-unillm-ratelimit-remaining", rate.remaining);
    set_numeric_header(headers, "x-unillm-ratelimit-reset", rate.reset_seconds);
}

fn set_numeric_header(headers: &mut HeaderMap, name: &str, value: u64) {
    let Ok(hv) = HeaderValue::from_str(&value.to_string()) else {
        return;
    };
    if let Ok(hn) = name.parse::<HeaderName>() {
        headers.insert(hn, hv);
    }
}

/// A 429 response with `Retry-After` + `X-Unillm-RateLimit-*` (`DESIGN.md` §12.3).
pub fn rate_limited_response(retry_after: Duration, headers: RateHeaders) -> Response {
    let mut resp = (
        StatusCode::TOO_MANY_REQUESTS,
        Json(json!({ "error": { "kind": "rate_limited", "message": "rate limit exceeded" } })),
    )
        .into_response();
    let h = resp.headers_mut();
    if let Ok(hv) = HeaderValue::from_str(&retry_after.as_secs().to_string()) {
        h.insert("retry-after", hv);
    }
    apply_rate_headers(h, &headers);
    resp
}

/// A guard that releases the held concurrency slot when dropped. It is moved into a streaming
/// response body so the slot frees when the stream completes OR the client disconnects (mirrors the
/// M3.4 cancellation path). `release` is async, so `Drop` spawns it on the runtime.
pub struct ReleaseGuard {
    limiter: Arc<dyn RateLimiter>,
    key_id: Uuid,
    limits: KeyLimits,
}

impl ReleaseGuard {
    pub fn new(limiter: Arc<dyn RateLimiter>, key_id: Uuid, limits: KeyLimits) -> Self {
        Self {
            limiter,
            key_id,
            limits,
        }
    }
}

impl Drop for ReleaseGuard {
    fn drop(&mut self) {
        let limiter = self.limiter.clone();
        let key_id = self.key_id;
        let limits = self.limits;
        // Best-effort: at process shutdown there may be no runtime; a leaked in-flight slot is
        // harmless for a per-instance in-memory limiter.
        tokio::spawn(async move {
            limiter.release(key_id, &limits, None).await;
        });
    }
}
