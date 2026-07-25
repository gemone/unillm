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

    Config {
        bind,
        routes: Routes::new(),
        upstreams,
    }
}
