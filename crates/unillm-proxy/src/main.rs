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

/// The four storage trait objects the proxy wires into [`Stores`].
type StoreArcs = (
    Arc<dyn KeyStore>,
    Arc<dyn RouteStore>,
    Arc<dyn ModelStore>,
    Arc<dyn LogStore>,
);

/// Open the storage backends for `database_url` (`DESIGN.md` §11.2): `postgres://` → `PostgresStore`
/// (requires the `postgres` feature), else `SqliteStore`. Migrations apply unless `run_migrations`
/// is false (`DESIGN.md` §21).
async fn open_stores(url: &str, run_migrations: bool) -> Result<StoreArcs, String> {
    if url.starts_with("postgres") {
        open_postgres(url, run_migrations).await
    } else {
        open_sqlite(url, run_migrations).await
    }
}

#[cfg(feature = "postgres")]
async fn open_postgres(url: &str, run_migrations: bool) -> Result<StoreArcs, String> {
    let s = if run_migrations {
        unillm_storage::PostgresStore::connect(url).await
    } else {
        unillm_storage::PostgresStore::connect_without_migrations(url).await
    }
    .map_err(|e| e.to_string())?;
    let s = Arc::new(s);
    Ok((s.clone(), s.clone(), s.clone(), s))
}

#[cfg(not(feature = "postgres"))]
async fn open_postgres(url: &str, _run_migrations: bool) -> Result<StoreArcs, String> {
    Err(format!(
        "database_url '{url}' is PostgreSQL but the proxy was built without the `postgres` feature; \
         rebuild with --features postgres"
    ))
}

async fn open_sqlite(url: &str, run_migrations: bool) -> Result<StoreArcs, String> {
    let s = if run_migrations {
        SqliteStore::connect(url).await
    } else {
        SqliteStore::connect_without_migrations(url).await
    }
    .map_err(|e| e.to_string())?;
    let s = Arc::new(s);
    Ok((s.clone(), s.clone(), s.clone(), s))
}

/// Load config, open storage + seed a dev key, build per-provider backend clients, and serve.
async fn serve() {
    let cfg = config::from_env();

    let (keys, routes, models, logs) =
        match open_stores(&cfg.database_url, cfg.run_migrations).await {
            Ok(s) => s,
            Err(e) => {
                eprintln!("FATAL: could not open storage at {}: {e}", cfg.database_url);
                return;
            }
        };

    // Dev bootstrap: seed a key from UNILLM_SEED_KEY so the data plane is callable before the admin
    // REST API exists (M4.5). No-op if the key already exists.
    if let Some(secret) = &cfg.seed_key {
        let hash = hash_secret(secret, &cfg.key_pepper);
        if keys.find_by_hash(&hash).await.unwrap_or(None).is_none() {
            let scopes = SEED_SCOPES.iter().map(|s| (*s).to_string()).collect();
            match keys
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
