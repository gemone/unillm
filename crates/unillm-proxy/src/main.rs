//! unillm-proxy entrypoint: load env config, open storage + seed a dev key, build per-provider
//! backend clients, serve.

use std::collections::HashMap;
use std::sync::Arc;

use unillm_core::{Client as CoreClient, ProviderConfig};
use unillm_storage::{
    InMemoryCache, InMemoryRateLimiter, KeyStore, LogStore, ModelStore, NewVirtualKey, RouteStore,
    SqliteStore, hash_secret, key_prefix,
};
use uuid::Uuid;

use clap::Parser;

use unillm_proxy::cli::{Cli, TopCmd};
use unillm_proxy::config;
use unillm_proxy::metrics::Metrics;
use unillm_proxy::server::{AppState, Stores, build_app};

/// Scopes granted to an env-seeded dev key (`DESIGN.md` §13.1).
const SEED_SCOPES: &[&str] = &["data", "admin", "read-usage"];

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    match cli.command {
        Some(TopCmd::Admin { cmd }) => {
            if let Err(e) = unillm_proxy::cli::run_admin(cmd).await {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        }
        Some(TopCmd::Serve) | None => serve().await,
    }
}

/// Load config, open storage + seed a dev key, build per-provider backend clients, and serve.
async fn serve() {
    let cfg = config::from_env();

    let store = match if cfg.run_migrations {
        SqliteStore::connect(&cfg.database_url).await
    } else {
        SqliteStore::connect_without_migrations(&cfg.database_url).await
    } {
        Ok(s) => Arc::new(s),
        Err(e) => {
            eprintln!("FATAL: could not open storage at {}: {e}", cfg.database_url);
            return;
        }
    };

    // Dev bootstrap: seed a key from UNILLM_SEED_KEY so the data plane is callable before the admin
    // REST API exists (M4.5). No-op if the key already exists.
    if let Some(secret) = &cfg.seed_key {
        let hash = hash_secret(secret, &cfg.key_pepper);
        if store.find_by_hash(&hash).await.unwrap_or(None).is_none() {
            let scopes = SEED_SCOPES.iter().map(|s| (*s).to_string()).collect();
            match store
                .create_key(NewVirtualKey {
                    key_hash: hash,
                    key_prefix: key_prefix(secret),
                    tenant_id: Uuid::new_v4(),
                    scopes,
                    model_allowlist: None,
                    budget_daily_tokens: None,
                    rpm: None,
                    tpm: None,
                    max_concurrency: None,
                    expires_at: None,
                })
                .await
            {
                Ok(_) => eprintln!("seeded dev key: {secret}"),
                Err(e) => eprintln!("WARNING: could not seed dev key: {e}"),
            }
        }
    }

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

    let keys: Arc<dyn KeyStore> = store.clone();
    let routes: Arc<dyn RouteStore> = store.clone();
    let models: Arc<dyn ModelStore> = store.clone();
    let logs: Arc<dyn LogStore> = store;
    let stores = Stores {
        keys,
        routes,
        models,
        logs,
        rate_limiter: Arc::new(InMemoryRateLimiter::new()),
        cache: Arc::new(InMemoryCache::new()),
    };
    let state = AppState::new(
        clients,
        stores,
        cfg.key_pepper,
        cfg.admin_token,
        cfg.limits,
        cfg.cache,
        Arc::new(Metrics::new()),
    );
    let app = build_app(state);
    let listener = tokio::net::TcpListener::bind(cfg.bind)
        .await
        .expect("failed to bind");
    eprintln!("unillm-proxy listening on {}", cfg.bind);
    axum::serve(listener, app).await.expect("server error");
}
