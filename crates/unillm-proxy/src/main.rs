//! unillm-proxy entrypoint: load env config, build per-provider backend clients, serve.

use std::collections::HashMap;
use std::sync::Arc;

use unillm_core::{Client as CoreClient, ProviderConfig};

use unillm_proxy::config;
use unillm_proxy::server::{AppState, build_app};

#[tokio::main]
async fn main() {
    let cfg = config::from_env();

    let mut clients = HashMap::new();
    for (provider, up) in &cfg.upstreams {
        let mut pc = ProviderConfig::new(*provider, &up.api_key);
        pc.base_url = up.base_url.clone();
        match CoreClient::new(pc) {
            Ok(c) => {
                clients.insert(*provider, Arc::new(c));
            }
            Err(e) => {
                eprintln!("WARNING: could not build client for {provider:?}: {e}");
            }
        }
    }
    if clients.is_empty() {
        eprintln!("WARNING: no upstream providers configured (set UNILLM_PROV_*_KEY env vars)");
    }

    let state = AppState::new(cfg.routes, clients);
    let app = build_app(state);
    let listener = tokio::net::TcpListener::bind(cfg.bind)
        .await
        .expect("failed to bind");
    eprintln!("unillm-proxy listening on {}", cfg.bind);
    axum::serve(listener, app).await.expect("server error");
}
