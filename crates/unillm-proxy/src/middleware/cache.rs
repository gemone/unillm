//! Exact-hash response cache (`DESIGN.md` §7.4): the inline pipeline stages for lookup
//! (`DESIGN.md` §10.3 step 5) and store (§10.3 step 9).
//!
//! **Key** = `sha256(virtual_key_scope || NUL || canonical_request_minus_metadata)`. The `metadata`
//! map is dropped (`DESIGN.md` §7.4: "canonical_request_minus_metadata") so the same logical request
//! hits regardless of caller-supplied tags; the **virtual-key scope** (`virtual_key_id`) is mixed in so
//! equal requests under different keys never collide — no cross-key leakage. The outbound format is
//! **deliberately not** part of the key: the cached value is the canonical `Response`, translated to
//! whatever outbound format the client wants on egress, so one entry serves every format. (`DESIGN.md`
//! §7.4 specifies this exact key; it overrides the plan's note that the format should be included.)
//!
//! **Value** = the serialized canonical `Response`. Only non-streaming 2xx responses are cached
//! (`DESIGN.md` §7.4, §10.3 step 5); streams bypass the cache entirely. A hit releases the rate-limit
//! concurrency slot (no upstream call) and is logged with `cached = true` and zero usage — the tokens
//! were spent on the original miss, not re-billed (`DESIGN.md` §13.5 avoids double-counting).

use std::env;
use std::time::Duration;

use serde_json::Value;
use sha2::{Digest, Sha256};

use unillm_core::Response;
use unillm_core::ir::Request as CanonicalRequest;

/// `DESIGN.md` §14.1 cache env: enabled flag + TTL.
#[derive(Debug, Clone, Copy)]
pub struct CacheConfig {
    pub enabled: bool,
    pub ttl: Duration,
}

impl CacheConfig {
    /// Read `UNILLM_CACHE_ENABLED` (default `false`) and `UNILLM_CACHE_TTL` (default `300`s) —
    /// the response cache is opt-in (`DESIGN.md` §14.1).
    pub fn from_env() -> Self {
        let enabled = env::var("UNILLM_CACHE_ENABLED")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let ttl = env::var("UNILLM_CACHE_TTL")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|&s: &u64| s > 0)
            .map(Duration::from_secs)
            .unwrap_or_else(|| Duration::from_secs(300));
        Self { enabled, ttl }
    }

    /// Disabled cache (for tests / when unset).
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ttl: Duration::from_secs(300),
        }
    }
}

/// `true` if this request is cacheable: the response cache is on and the request is non-streaming
/// (`DESIGN.md` §7.4 — streams are never cached in v1).
pub fn cacheable(cfg: CacheConfig, req: &CanonicalRequest) -> bool {
    cfg.enabled && !req.stream
}

/// The deterministic cache key (`DESIGN.md` §7.4). See the module docs for the exact composition;
/// the canonical-JSON normalization makes the fingerprint independent of field/insertion order.
pub fn cache_key(req: &CanonicalRequest, scope: &str) -> String {
    // §7.4: hash the canonical request with `metadata` dropped. Serialize once to a `Value` and
    // remove the key — cheaper than cloning the whole `Request` (input, tools, …) to clear a field.
    let mut value = serde_json::to_value(req).unwrap_or(Value::Null);
    if let Some(map) = value.as_object_mut() {
        map.remove("metadata");
    }
    let mut hasher = Sha256::new();
    hasher.update(scope.as_bytes());
    hasher.update(b"\x00"); // delimiter so the scope can't bleed into the request bytes
    hasher.update(canonicalize(&value).as_bytes());
    hex::encode(hasher.finalize())
}

/// Serialize a canonical response for caching (`DESIGN.md` §7.4 value = canonical `Response`).
pub fn encode_response(resp: &Response) -> Vec<u8> {
    serde_json::to_vec(resp).unwrap_or_default()
}

/// Deserialize a cached canonical response. `None` if the bytes are corrupt (treated as a miss).
pub fn decode_response(bytes: &[u8]) -> Option<Response> {
    serde_json::from_slice(bytes).ok()
}

/// Deterministic compact JSON for hashing: object keys sorted ascending, no whitespace. Makes the
/// fingerprint independent of serde_json's map ordering (BTreeMap by default, but not guaranteed).
fn canonicalize(v: &Value) -> String {
    let mut out = String::new();
    canon(v, &mut out);
    out
}

fn canon(v: &Value, out: &mut String) {
    match v {
        Value::Object(map) => {
            out.push('{');
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            for (i, k) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&serde_json::to_string(k).unwrap()); // "key", escaped
                out.push(':');
                canon(&map[*k], out);
            }
            out.push('}');
        }
        Value::Array(a) => {
            out.push('[');
            for (i, e) in a.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                canon(e, out);
            }
            out.push(']');
        }
        // Numbers/bools/null/strings render canonically via serde_json (strings get quoted+escaped).
        other => out.push_str(&other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use unillm_core::Usage;
    use unillm_core::ir::{ModelRef, Request};

    fn req(model: &str) -> Request {
        Request {
            model: ModelRef::Alias(model.into()),
            instructions: None,
            input: vec![],
            max_tokens: None,
            temperature: None,
            top_p: None,
            stop: None,
            tools: None,
            tool_choice: None,
            stream: false,
            cache: Default::default(),
            metadata: Default::default(),
        }
    }

    #[test]
    fn metadata_is_excluded_from_key() {
        let mut a = req("gpt-4o");
        a.metadata.insert("request_id".into(), "abc".into());
        let mut b = req("gpt-4o");
        b.metadata.insert("request_id".into(), "different".into());
        // Same logical request, different metadata → same key (§7.4).
        assert_eq!(cache_key(&a, "scope-1"), cache_key(&b, "scope-1"));
    }

    #[test]
    fn scope_isolates_keys() {
        let r = req("gpt-4o");
        // Same request under different virtual keys → different keys (no cross-key leakage).
        assert_ne!(cache_key(&r, "key-A"), cache_key(&r, "key-B"));
    }

    #[test]
    fn different_requests_differ() {
        assert_ne!(
            cache_key(&req("gpt-4o"), "k"),
            cache_key(&req("claude"), "k")
        );
    }

    #[test]
    fn key_is_order_independent() {
        // Input field order / map insertion order must not change the fingerprint.
        let mut a = req("gpt-4o");
        a.temperature = Some(0.5);
        a.max_tokens = Some(100);
        let mut b = req("gpt-4o");
        b.max_tokens = Some(100);
        b.temperature = Some(0.5);
        assert_eq!(cache_key(&a, "k"), cache_key(&b, "k"));
    }

    #[test]
    fn encode_decode_round_trip() {
        let resp = Response {
            id: "r1".into(),
            model: "gpt-4o".into(),
            provider: unillm_core::ProviderId::Openai,
            output: vec![],
            stop_reason: unillm_core::ir::StopReason::EndTurn,
            usage: Usage::zero(),
        };
        let bytes = encode_response(&resp);
        let back = decode_response(&bytes).unwrap();
        assert_eq!(back.id, "r1");
        assert_eq!(back.model, "gpt-4o");
    }

    #[test]
    fn cacheable_respects_stream_and_enabled() {
        let on = CacheConfig {
            enabled: true,
            ttl: Duration::from_secs(60),
        };
        let off = CacheConfig::disabled();
        let mut r = req("gpt-4o");
        assert!(cacheable(on, &r));
        assert!(!cacheable(off, &r));
        r.stream = true;
        assert!(!cacheable(on, &r)); // streams never cached
    }
}
