//! Cache control and usage normalization (`DESIGN.md` §7).
//!
//! `normalize_usage` maps each provider's native usage object onto the canonical [`Usage`],
//! preserving the invariant `input_tokens + cache_read + cache_creation == provider's total prompt
//! tokens` (`DESIGN.md` §4.7). Breakpoint application (`apply_cache`) for the Anthropic explicit
//! strategy arrives with the Anthropic adapter in M1.3.

use serde_json::Value;

use crate::ir::{ProviderId, Usage};

fn u64_at(v: &Value, path: &[&str]) -> u64 {
    let mut cur = v;
    for p in path {
        cur = match cur.get(*p) {
            Some(x) => x,
            None => return 0,
        };
    }
    cur.as_u64().unwrap_or(0)
}

/// OpenAI / OpenRouter: `cache_read = prompt_tokens_details.cached_tokens`;
/// `input_tokens = prompt_tokens - cached_tokens` (`DESIGN.md` §7.2).
fn cc_input_usage(u: &Value) -> (u64, u64) {
    let prompt = u64_at(u, &["prompt_tokens"]);
    let cached = u64_at(u, &["prompt_tokens_details", "cached_tokens"]);
    (prompt.saturating_sub(cached), cached)
}

/// Normalize a provider-native usage object into canonical [`Usage`] (`DESIGN.md` §7.2).
///
/// Missing fields default to 0 (DeepSeek/OpenRouter cache fields may be absent — best-effort).
pub fn normalize_usage(provider: ProviderId, u: &Value) -> Usage {
    let output_tokens = u64_at(u, &["completion_tokens"]).max(u64_at(u, &["output_tokens"]));
    let cost_usd = u.get("cost").and_then(|v| v.as_f64());

    let (input_tokens, cache_read, cache_creation) = match provider {
        // DeepSeek reports explicit hit/miss counters.
        ProviderId::Deepseek => {
            let hit = u64_at(u, &["prompt_cache_hit_tokens"]);
            let miss = u64_at(u, &["prompt_cache_miss_tokens"]);
            (miss, hit, 0)
        }
        // Anthropic reports input (excludes cached) plus read/creation counters directly.
        ProviderId::Anthropic => (
            u64_at(u, &["input_tokens"]),
            u64_at(u, &["cache_read_input_tokens"]),
            u64_at(u, &["cache_creation_input_tokens"]),
        ),
        // OpenAI and OpenRouter use the CC cached_tokens detail. OpenRouter may add `cost`.
        ProviderId::Openai | ProviderId::Openrouter => {
            let (input, cache_read) = cc_input_usage(u);
            (input, cache_read, 0)
        }
    };

    Usage {
        input_tokens,
        output_tokens,
        cache_read,
        cache_creation,
        // Only OpenRouter quotes cost; others stay None until the proxy prices them (M4/M5).
        cost_usd: if provider == ProviderId::Openrouter {
            cost_usd
        } else {
            None
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn openai_invariant() {
        let u = normalize_usage(
            ProviderId::Openai,
            &json!({"prompt_tokens":100,"completion_tokens":40,"prompt_tokens_details":{"cached_tokens":30}}),
        );
        assert_eq!(u.input_tokens, 70);
        assert_eq!(u.cache_read, 30);
        assert_eq!(u.output_tokens, 40);
        assert_eq!(u.total_input(), 100); // input + cache_read + cache_creation
    }

    #[test]
    fn deepseek_hit_miss() {
        let u = normalize_usage(
            ProviderId::Deepseek,
            &json!({"prompt_tokens":100,"completion_tokens":40,"prompt_cache_hit_tokens":25,"prompt_cache_miss_tokens":75}),
        );
        assert_eq!(u.cache_read, 25);
        assert_eq!(u.input_tokens, 75);
        assert_eq!(u.total_input(), 100);
    }

    #[test]
    fn anthropic_direct() {
        let u = normalize_usage(
            ProviderId::Anthropic,
            &json!({"input_tokens":50,"output_tokens":40,"cache_read_input_tokens":20,"cache_creation_input_tokens":10}),
        );
        assert_eq!(u.input_tokens, 50);
        assert_eq!(u.cache_read, 20);
        assert_eq!(u.cache_creation, 10);
        assert_eq!(u.total_input(), 80);
    }

    #[test]
    fn openrouter_cost() {
        let u = normalize_usage(
            ProviderId::Openrouter,
            &json!({"prompt_tokens":100,"completion_tokens":40,"prompt_tokens_details":{"cached_tokens":10},"cost":0.0021}),
        );
        assert_eq!(u.input_tokens, 90);
        assert_eq!(u.cache_read, 10);
        assert_eq!(u.cost_usd, Some(0.0021));
    }

    #[test]
    fn missing_cache_fields_default_to_zero() {
        let u = normalize_usage(
            ProviderId::Openai,
            &json!({"prompt_tokens":10,"completion_tokens":5}),
        );
        assert_eq!(u.cache_read, 0);
        assert_eq!(u.input_tokens, 10);
    }
}
