//! Rate-limit response helpers + the stream-release guard (`DESIGN.md` §12).

use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use serde_json::json;
use uuid::Uuid;

use unillm_storage::{KeyLimits, RateHeaders, RateLimiter, TokenActual, TokenEstimate};

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

/// A guard that releases the held concurrency slot when dropped. It serves two roles:
///   * moved into a streaming response body, it frees the slot when the stream completes OR the
///     client disconnects (mirrors the M3.4 cancellation path);
///   * held in the non-stream handler scope, it guarantees the slot is released on **every** exit
///     (early validation errors, all-failed, success) without a manual `release` at each return.
///
/// `release` is async, so `Drop` spawns it on the runtime. Use [`ReleaseGuard::complete`] to release
/// with known usage (success) or [`ReleaseGuard::disarm`] when handing the slot off (the stream path
/// owns its own body-scoped guard).
pub struct ReleaseGuard {
    limiter: Arc<dyn RateLimiter>,
    key_id: Uuid,
    limits: KeyLimits,
    armed: bool,
}

impl ReleaseGuard {
    pub fn new(limiter: Arc<dyn RateLimiter>, key_id: Uuid, limits: KeyLimits) -> Self {
        Self {
            limiter,
            key_id,
            limits,
            armed: true,
        }
    }

    /// Release the slot with actual usage and disarm so `Drop` does not release again. Idempotent.
    /// Use on the successful non-stream path where token usage is known.
    pub async fn complete(&mut self, actual: Option<TokenActual>) {
        if self.armed {
            self.limiter
                .release(self.key_id, &self.limits, actual)
                .await;
            self.armed = false;
        }
    }

    /// Disarm without releasing — for the streaming path, which moves its own body-scoped guard in
    /// to release when the stream ends (so the slot is neither double-released nor leaked).
    pub fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ReleaseGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderMap;

    #[test]
    fn estimate_tokens_is_bytes_over_four_plus_max_output() {
        let e = estimate_tokens(8, Some(50));
        assert_eq!(e.prompt, 2);
        assert_eq!(e.max_output, 50);
        // No max_tokens → default 1024; empty body → 0 prompt tokens.
        let e = estimate_tokens(0, None);
        assert_eq!(e.prompt, 0);
        assert_eq!(e.max_output, 1024);
    }

    #[test]
    fn apply_rate_headers_sets_all_three() {
        let mut h = HeaderMap::new();
        apply_rate_headers(
            &mut h,
            &RateHeaders {
                limit: 100,
                remaining: 7,
                reset_seconds: 30,
            },
        );
        assert_eq!(h.get("x-unillm-ratelimit-limit").unwrap(), "100");
        assert_eq!(h.get("x-unillm-ratelimit-remaining").unwrap(), "7");
        assert_eq!(h.get("x-unillm-ratelimit-reset").unwrap(), "30");
    }
}
