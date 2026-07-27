//! Configuration from the environment (`DESIGN.md` §14).

use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;

use unillm_core::ProviderId;

/// An upstream provider's credentials and address.
#[derive(Debug, Clone)]
pub struct Upstream {
    pub api_key: String,
    pub base_url: String,
}

/// Proxy-level request caps (`DESIGN.md` §16: max input items, tools, output tokens).
#[derive(Debug, Clone, Copy)]
pub struct RequestLimits {
    pub max_input_items: usize,
    pub max_tools: usize,
    pub max_output_tokens: u32,
}

impl RequestLimits {
    /// Development defaults; overridable via `UNILLM_MAX_*` env vars.
    fn from_env() -> Self {
        Self {
            max_input_items: env::var("UNILLM_MAX_INPUT_ITEMS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1000),
            max_tools: env::var("UNILLM_MAX_TOOLS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(128),
            max_output_tokens: env::var("UNILLM_MAX_OUTPUT_TOKENS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(16_384),
        }
    }
}

/// Proxy configuration.
#[derive(Debug, Clone)]
pub struct Config {
    pub bind: SocketAddr,
    pub upstreams: HashMap<ProviderId, Upstream>,
    /// sqlx URL (`sqlite:...` now, `postgres://...` in M4.5). `DESIGN.md` §14.1.
    pub database_url: String,
    /// Distinct token gating `/admin/*` (`DESIGN.md` §10.6, §16). `None` disables admin endpoints.
    pub admin_token: Option<String>,
    /// Pepper mixed into virtual-key hashes (D11).
    pub key_pepper: String,
    /// If set, a dev key with this secret is seeded at startup (broad scopes, no limits).
    pub seed_key: Option<String>,
    /// Inbound request caps (`DESIGN.md` §16).
    pub limits: RequestLimits,
    /// Exact-hash response cache (`DESIGN.md` §7.4, §14.1). Opt-in; off by default.
    pub cache: crate::middleware::cache::CacheConfig,
}

const DEFAULT_BIND: &str = "0.0.0.0:8080";

/// `(key env var, default base URL)` per provider.
fn provider_env(provider: ProviderId) -> (&'static str, &'static str) {
    match provider {
        ProviderId::Openai => ("UNILLM_PROV_OPENAI_KEY", "https://api.openai.com/v1"),
        ProviderId::Anthropic => ("UNILLM_PROV_ANTHROPIC_KEY", "https://api.anthropic.com/v1"),
        ProviderId::Openrouter => ("UNILLM_PROV_OPENROUTER_KEY", "https://openrouter.ai/api/v1"),
        ProviderId::Deepseek => ("UNILLM_PROV_DEEPSEEK_KEY", "https://api.deepseek.com"),
    }
}

/// Load configuration from the environment (`DESIGN.md` §14.1).
///
/// Upstreams are enabled per provider by setting `UNILLM_PROV_<PROVIDER>_KEY` (with an optional
/// `..._BASE_URL` override). Routes live in the DB `routes` table (M4.3).
pub fn from_env() -> Config {
    let bind = env::var("UNILLM_PROXY_BIND")
        .unwrap_or_else(|_| DEFAULT_BIND.into())
        .parse()
        .unwrap_or_else(|_| DEFAULT_BIND.parse().unwrap());

    let mut upstreams = HashMap::new();
    for provider in [
        ProviderId::Openai,
        ProviderId::Anthropic,
        ProviderId::Openrouter,
        ProviderId::Deepseek,
    ] {
        let (key_var, default_base) = provider_env(provider);
        if let Ok(api_key) = env::var(key_var) {
            let base_var = format!("{}_BASE_URL", key_var.trim_end_matches("_KEY"));
            let base_url = env::var(&base_var).unwrap_or_else(|_| default_base.into());
            upstreams.insert(provider, Upstream { api_key, base_url });
        }
    }

    let database_url =
        env::var("UNILLM_DATABASE_URL").unwrap_or_else(|_| "sqlite:unillm.db".into());
    let admin_token = env::var("UNILLM_ADMIN_TOKEN").ok();
    let key_pepper = match env::var("UNILLM_KEY_PEPPER") {
        Ok(p) => p,
        Err(_) => {
            eprintln!(
                "WARNING: UNILLM_KEY_PEPPER unset; using insecure default. Set it in production."
            );
            "unillm-insecure-dev-pepper".into()
        }
    };
    let seed_key = env::var("UNILLM_SEED_KEY").ok();

    Config {
        bind,
        upstreams,
        database_url,
        admin_token,
        key_pepper,
        seed_key,
        limits: RequestLimits::from_env(),
        cache: crate::middleware::cache::CacheConfig::from_env(),
    }
}
