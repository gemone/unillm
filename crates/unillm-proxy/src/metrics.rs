//! Prometheus metrics (`DESIGN.md` §17). Hand-rolled — atomic counters + a latency histogram under
//! a `Mutex<BTreeMap>`, rendered to the Prometheus text exposition format. This avoids the `metrics`
//! crate's global recorder (which fights our per-test server harness) and adds no dependencies.
//!
//! **Labels:** `provider`, `model`, `status`, and `cache` outcome (bounded cardinality — model
//! labels derive from the catalog). Per-key / per-tenant granularity is **not** exposed here:
//! virtual-key ids are unbounded and would explode the registry; that dimension lives in the usage
//! DB (`DESIGN.md` §13.5). Token/cost totals reflect only requests that reported usage inline
//! (non-stream completions + cache misses); stream token totals are recorded from the stream logger
//! at completion.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use unillm_storage::NewUsage;

/// Standard Prometheus latency buckets (seconds), covering 5ms–10s.
const LATENCY_BUCKETS_SEC: [f64; 11] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// The cache outcome for a request, used as a metric label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheOutcome {
    /// Cache disabled or not consulted (errors, streams).
    None,
    /// Cacheable request that did not hit (populated the cache).
    Miss,
    /// Served from the cache.
    Hit,
}

impl CacheOutcome {
    fn as_str(self) -> &'static str {
        match self {
            CacheOutcome::None => "none",
            CacheOutcome::Miss => "miss",
            CacheOutcome::Hit => "hit",
        }
    }
}

#[derive(Default)]
struct Histogram {
    /// Cumulative bucket counts: `buckets[i]` = observations ≤ `LATENCY_BUCKETS_SEC[i]`.
    buckets: [u64; LATENCY_BUCKETS_SEC.len()],
    count: u64,
    sum: f64,
}

impl Histogram {
    fn observe(&mut self, secs: f64) {
        for (i, &le) in LATENCY_BUCKETS_SEC.iter().enumerate() {
            if secs <= le {
                self.buckets[i] += 1;
            }
        }
        self.count += 1;
        self.sum += secs;
    }
}

#[derive(Default)]
struct MetricsInner {
    requests: BTreeMap<(String, String, i16, &'static str), u64>,
    tokens: BTreeMap<(String, String, &'static str), u64>,
    cost: BTreeMap<(String, String), f64>,
    latency: BTreeMap<(String, String), Histogram>,
}

/// Proxy metrics registry (`DESIGN.md` §17). Cheap to clone behind an `Arc`; one instance per
/// process, shared across all data-plane requests.
#[derive(Default)]
pub struct Metrics {
    inner: Mutex<MetricsInner>,
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record one completed request: its outcome, latency, and (when available) token usage + cost.
    /// Provider/model are the snake_case provider id and native model that answered (or, for a cache
    /// hit, the cached response's). Best-effort — never panics, never blocks the request.
    pub fn record(
        &self,
        provider: &str,
        model: &str,
        status: i16,
        cache: CacheOutcome,
        latency: Duration,
        usage: Option<&NewUsage>,
    ) {
        let mut inner = self.inner.lock().unwrap();
        *inner
            .requests
            .entry((provider.into(), model.into(), status, cache.as_str()))
            .or_default() += 1;
        if let Some(u) = usage {
            for (kind, n) in [
                ("input", u.input_tokens),
                ("output", u.output_tokens),
                ("cache_read", u.cache_read),
                ("cache_creation", u.cache_creation),
            ] {
                if n > 0 {
                    *inner
                        .tokens
                        .entry((provider.into(), model.into(), kind))
                        .or_default() += n as u64;
                }
            }
            if let Some(c) = u.cost_usd
                && c > 0.0
            {
                *inner
                    .cost
                    .entry((provider.into(), model.into()))
                    .or_default() += c;
            }
        }
        inner
            .latency
            .entry((provider.into(), model.into()))
            .or_default()
            .observe(latency.as_secs_f64());
    }

    /// Render the registry to the Prometheus text exposition format (`DESIGN.md` §17 `/metrics`).
    pub fn render(&self) -> String {
        let inner = self.inner.lock().unwrap();
        let mut out = String::new();

        out.push_str("# HELP unillm_requests_total Total data-plane requests.\n");
        out.push_str("# TYPE unillm_requests_total counter\n");
        for ((provider, model, status, cache), n) in &inner.requests {
            out.push_str(&format!(
                "unillm_requests_total{{provider=\"{provider}\",model=\"{model}\",status=\"{status}\",cache=\"{cache}\"}} {n}\n"
            ));
        }

        out.push_str("# HELP unillm_tokens_total Tokens consumed, by kind.\n");
        out.push_str("# TYPE unillm_tokens_total counter\n");
        for ((provider, model, kind), n) in &inner.tokens {
            out.push_str(&format!(
                "unillm_tokens_total{{provider=\"{provider}\",model=\"{model}\",kind=\"{kind}\"}} {n}\n"
            ));
        }

        out.push_str(
            "# HELP unillm_cost_usd_total Estimated upstream cost in USD (provider-supplied).\n",
        );
        out.push_str("# TYPE unillm_cost_usd_total counter\n");
        for ((provider, model), c) in &inner.cost {
            out.push_str(&format!(
                "unillm_cost_usd_total{{provider=\"{provider}\",model=\"{model}\"}} {c}\n"
            ));
        }

        out.push_str("# HELP unillm_request_duration_seconds Request latency distribution.\n");
        out.push_str("# TYPE unillm_request_duration_seconds histogram\n");
        for ((provider, model), h) in &inner.latency {
            for (i, &le) in LATENCY_BUCKETS_SEC.iter().enumerate() {
                out.push_str(&format!(
                    "unillm_request_duration_seconds_bucket{{provider=\"{provider}\",model=\"{model}\",le=\"{le}\"}} {}\n",
                    h.buckets[i]
                ));
            }
            out.push_str(&format!(
                "unillm_request_duration_seconds_bucket{{provider=\"{provider}\",model=\"{model}\",le=\"+Inf\"}} {}\n",
                h.count
            ));
            out.push_str(&format!(
                "unillm_request_duration_seconds_count{{provider=\"{provider}\",model=\"{model}\"}} {}\n",
                h.count
            ));
            out.push_str(&format!(
                "unillm_request_duration_seconds_sum{{provider=\"{provider}\",model=\"{model}\"}} {}\n",
                h.sum
            ));
        }

        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_is_valid_prometheus_and_counts() {
        let m = Metrics::new();
        m.record(
            "openai",
            "gpt-4o",
            200,
            CacheOutcome::Miss,
            Duration::from_millis(120),
            Some(&NewUsage {
                input_tokens: 10,
                output_tokens: 5,
                cache_read: 0,
                cache_creation: 0,
                cost_usd: Some(0.0001),
            }),
        );
        m.record(
            "openai",
            "gpt-4o",
            200,
            CacheOutcome::Hit,
            Duration::from_millis(2),
            None,
        );

        let text = m.render();
        // Required Prometheus framing.
        assert!(text.contains("# TYPE unillm_requests_total counter"));
        assert!(text.contains("# TYPE unillm_tokens_total counter"));
        assert!(text.contains("# TYPE unillm_request_duration_seconds histogram"));
        // Two requests recorded (one miss, one hit).
        assert!(text.contains("cache=\"miss\"} 1"));
        assert!(text.contains("cache=\"hit\"} 1"));
        // Tokens only from the miss (the hit recorded no usage).
        assert!(text.contains("kind=\"input\"} 10"));
        assert!(text.contains("kind=\"output\"} 5"));
        // Histogram has the +Inf bucket and a count.
        assert!(text.contains("le=\"+Inf\"} 2"));
        assert!(text.contains(
            "unillm_request_duration_seconds_count{provider=\"openai\",model=\"gpt-4o\"} 2"
        ));
        // Every metric line is `name{labels} value`.
        for line in text.lines() {
            if !line.starts_with('#') && !line.is_empty() {
                assert!(line.contains("} "), "malformed metric line: {line}");
            }
        }
    }
}
