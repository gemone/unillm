//! Configuration from the environment (`DESIGN.md` §14).

use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;

use unillm_core::ProviderId;

use crate::route::Routes;

/// An upstream provider's credentials and address.
#[derive(Debug, Clone)]
pub struct Upstream {
    pub api_key: String,
    pub base_url: String,
}

/// Proxy configuration.
#[derive(Debug, Clone)]
pub struct Config {
    pub bind: SocketAddr,
    pub routes: Routes,
    pub upstreams: HashMap<ProviderId, Upstream>,
    /// sqlx URL (`sqlite:...` now, `postgres://...` in M4.5). `DESIGN.md` §14.1.
    pub database_url: String,
    /// Distinct token gating `/admin/*` (`DESIGN.md` §10.6, §16). `None` disables admin endpoints.
    pub admin_token: Option<String>,
    /// Pepper mixed into virtual-key hashes (D11).
    pub key_pepper: String,
    /// If set, a dev key with this secret is seeded at startup (broad scopes, no limits).
    pub seed_key: Option<String>,
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
/// `..._BASE_URL` override). Routes are configured programmatically for now (the DB-backed routes
/// table arrives in M4).
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
        routes: Routes::new(),
        upstreams,
        database_url,
        admin_token,
        key_pepper,
        seed_key,
    }
}
